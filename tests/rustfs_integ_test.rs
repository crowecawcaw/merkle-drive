//! Integration tests that mount a merkle-drive virtual drive backed by a real
//! RustFS (S3-compatible) endpoint running in Docker via testcontainers.
//!
//! These tests exercise the full public interface: writing blobs, building tree
//! nodes, committing snapshots, reading them back, and testing multi-client
//! sync scenarios with CAS conflict handling.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use merkle_drive::commit::Commit;
use merkle_drive::hash;
use merkle_drive::storage::{S3Storage, Storage};
use merkle_drive::tree::*;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DATA_BUCKET: &str = "merkle-data";
const META_BUCKET: &str = "merkle-meta";

/// Start a RustFS container and return an S3Storage connected to it.
async fn start_rustfs_drive() -> (S3Storage, testcontainers::ContainerAsync<GenericImage>) {
    let container = GenericImage::new("rustfs/rustfs", "latest")
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Starting:"))
        .with_env_var("RUSTFS_ACCESS_KEY", "testuser")
        .with_env_var("RUSTFS_SECRET_KEY", "testpassword")
        .start()
        .await
        .expect("failed to start RustFS container");

    // Give RustFS a moment to fully initialize after the startup message
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("failed to get mapped port");

    let endpoint = format!("http://127.0.0.1:{port}");

    let creds = aws_credential_types::Credentials::new(
        "testuser",
        "testpassword",
        None,
        None,
        "test",
    );

    let config = aws_sdk_s3::Config::builder()
        .endpoint_url(&endpoint)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(creds)
        .force_path_style(true)
        .behavior_version_latest()
        .build();

    let client = aws_sdk_s3::Client::from_conf(config);

    // Create the two buckets
    client
        .create_bucket()
        .bucket(DATA_BUCKET)
        .send()
        .await
        .expect("failed to create data bucket");

    client
        .create_bucket()
        .bucket(META_BUCKET)
        .send()
        .await
        .expect("failed to create metadata bucket");

    let storage = S3Storage::new(
        client,
        DATA_BUCKET.to_string(),
        META_BUCKET.to_string(),
    );

    (storage, container)
}

/// Build a second S3Storage client pointing at the same RustFS container.
fn connect_second_client(port: u16) -> S3Storage {
    let endpoint = format!("http://127.0.0.1:{port}");

    let creds = aws_credential_types::Credentials::new(
        "testuser",
        "testpassword",
        None,
        None,
        "test",
    );

    let config = aws_sdk_s3::Config::builder()
        .endpoint_url(&endpoint)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(creds)
        .force_path_style(true)
        .behavior_version_latest()
        .build();

    let client = aws_sdk_s3::Client::from_conf(config);

    S3Storage::new(client, DATA_BUCKET.to_string(), META_BUCKET.to_string())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn make_leaf(entries: Vec<LeafEntry>) -> TreeNode {
    TreeNode::Leaf(LeafNode { entries })
}

fn inline_file(name: &str, data: &[u8]) -> LeafEntry {
    let ts = now_ns();
    LeafEntry::file(
        name.to_string(),
        data.len() as u64,
        FileContent::Inline(data.to_vec()),
        ts,
        ts,
        1000,
        1000,
        false,
    )
}

/// Helper: write a file as a blob, build a single-file root tree, commit it,
/// and return (root_hash_hex, etag).
async fn write_single_file(
    storage: &S3Storage,
    branch: &str,
    filename: &str,
    content: &[u8],
    parent_commit_hash: Option<[u8; 32]>,
    expected_etag: Option<&str>,
    client_id: &str,
    message: &str,
) -> (String, String) {
    let blob_hash_hex = storage.put_blob(content).await.unwrap();
    let blob_hash = hash::hex_decode(&blob_hash_hex).unwrap();

    let root = make_leaf(vec![LeafEntry::file(
        filename.to_string(),
        content.len() as u64,
        FileContent::Blocks(vec![blob_hash]),
        now_ns(),
        now_ns(),
        1000,
        1000,
        false,
    )]);

    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();

    let commit = Commit::new(
        root_hash,
        parent_commit_hash,
        client_id.to_string(),
        now_ms(),
        message.to_string(),
    );

    let etag = storage
        .put_head(branch, &commit, expected_etag)
        .await
        .unwrap();

    (root_hex, etag)
}

/// Read a file's blob content by navigating HEAD -> root tree -> entry -> blob.
async fn read_file(storage: &S3Storage, branch: &str, filename: &str) -> Vec<u8> {
    let (commit, _etag) = storage
        .get_head(branch)
        .await
        .unwrap()
        .expect("branch HEAD should exist");

    let root = storage.get_node(&commit.root).await.unwrap();

    match root.lookup_local(filename) {
        LookupResult::Found(entry) => match &entry.content {
            Some(FileContent::Inline(data)) => data.clone(),
            Some(FileContent::Blocks(blocks)) => {
                let mut result = Vec::new();
                for block in blocks {
                    let hex = hash::hex_encode(block);
                    let data = storage.get_blob(&hex).await.unwrap();
                    result.extend_from_slice(&data);
                }
                result
            }
            None => panic!("file entry has no content"),
        },
        other => panic!("expected Found for {filename}, got {other:?}"),
    }
}

// ============================================================
// Integration tests
// ============================================================

#[tokio::test]
async fn test_write_and_read_single_file() {
    let (storage, _container) = start_rustfs_drive().await;

    let content = b"hello from merkle-drive integration test!";

    write_single_file(
        &storage,
        "main",
        "hello.txt",
        content,
        None,
        None,
        "client-1",
        "initial commit",
    )
    .await;

    let read_back = read_file(&storage, "main", "hello.txt").await;
    assert_eq!(read_back, content);
}

#[tokio::test]
async fn test_write_read_inline_file() {
    let (storage, _container) = start_rustfs_drive().await;

    // Inline content (small, stored directly in tree node)
    let small_data = b"tiny";
    let root = make_leaf(vec![inline_file("small.txt", small_data)]);
    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();

    let commit = Commit::new(
        root_hash,
        None,
        "client-1".to_string(),
        now_ms(),
        "inline file".to_string(),
    );
    storage.put_head("main", &commit, None).await.unwrap();

    let read_back = read_file(&storage, "main", "small.txt").await;
    assert_eq!(read_back, small_data);
}

#[tokio::test]
async fn test_overwrite_file_new_commit() {
    let (storage, _container) = start_rustfs_drive().await;

    let (_, etag1) = write_single_file(
        &storage,
        "main",
        "data.txt",
        b"version 1",
        None,
        None,
        "client-1",
        "v1",
    )
    .await;

    // Read the commit hash for parent reference
    let (commit1, _) = storage.get_head("main").await.unwrap().unwrap();
    let commit1_hash = commit1.hash().unwrap();

    let (_root2, _etag2) = write_single_file(
        &storage,
        "main",
        "data.txt",
        b"version 2",
        Some(commit1_hash),
        Some(&etag1),
        "client-1",
        "v2",
    )
    .await;

    // Should read the latest version
    let read_back = read_file(&storage, "main", "data.txt").await;
    assert_eq!(read_back, b"version 2");

    // Verify commit chain
    let (head, _) = storage.get_head("main").await.unwrap().unwrap();
    assert_eq!(head.message, "v2");
    assert!(head.parent.is_some());
}

#[tokio::test]
async fn test_nested_directories() {
    let (storage, _container) = start_rustfs_drive().await;

    // Build src/main.rs
    let main_rs = b"fn main() { println!(\"hello\"); }";
    let main_rs_hash = storage.put_blob(main_rs).await.unwrap();
    let main_rs_hash_bytes = hash::hex_decode(&main_rs_hash).unwrap();

    let src_dir = make_leaf(vec![LeafEntry::file(
        "main.rs".to_string(),
        main_rs.len() as u64,
        FileContent::Blocks(vec![main_rs_hash_bytes]),
        now_ns(),
        now_ns(),
        1000,
        1000,
        false,
    )]);
    let src_hex = storage.put_node(&src_dir).await.unwrap();
    let src_hash = hash::hex_decode(&src_hex).unwrap();

    // Build docs/readme.md
    let readme = b"# Project Docs";
    let docs_dir = make_leaf(vec![inline_file("readme.md", readme)]);
    let docs_hex = storage.put_node(&docs_dir).await.unwrap();
    let docs_hash = hash::hex_decode(&docs_hex).unwrap();

    // Build root with Cargo.toml + src/ + docs/
    let root = make_leaf(vec![
        inline_file("Cargo.toml", b"[package]\nname = \"test\""),
        LeafEntry::dir("docs".to_string(), docs_hash),
        LeafEntry::dir("src".to_string(), src_hash),
    ]);
    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();

    let commit = Commit::new(
        root_hash,
        None,
        "client-1".to_string(),
        now_ms(),
        "project structure".to_string(),
    );
    storage.put_head("main", &commit, None).await.unwrap();

    // Navigate: HEAD -> root -> src/ -> main.rs -> blob
    let (head, _) = storage.get_head("main").await.unwrap().unwrap();
    let root_node = storage.get_node(&head.root).await.unwrap();

    // Check root has Cargo.toml
    match root_node.lookup_local("Cargo.toml") {
        LookupResult::Found(e) => {
            assert_eq!(e.entry_type, EntryType::File);
            match &e.content {
                Some(FileContent::Inline(data)) => {
                    assert_eq!(data, b"[package]\nname = \"test\"");
                }
                _ => panic!("expected inline content"),
            }
        }
        other => panic!("expected Found, got {other:?}"),
    }

    // Navigate into src/
    match root_node.lookup_local("src") {
        LookupResult::Found(entry) => {
            assert_eq!(entry.entry_type, EntryType::Dir);
            let src_node = storage
                .get_node(&hash::hex_encode(&entry.hash.unwrap()))
                .await
                .unwrap();

            match src_node.lookup_local("main.rs") {
                LookupResult::Found(file_entry) => {
                    if let Some(FileContent::Blocks(blocks)) = &file_entry.content {
                        let data = storage
                            .get_blob(&hash::hex_encode(&blocks[0]))
                            .await
                            .unwrap();
                        assert_eq!(data, main_rs);
                    } else {
                        panic!("expected blocks content");
                    }
                }
                other => panic!("expected Found for main.rs, got {other:?}"),
            }
        }
        other => panic!("expected Found for src, got {other:?}"),
    }

    // Navigate into docs/
    match root_node.lookup_local("docs") {
        LookupResult::Found(entry) => {
            let docs_node = storage
                .get_node(&hash::hex_encode(&entry.hash.unwrap()))
                .await
                .unwrap();

            match docs_node.lookup_local("readme.md") {
                LookupResult::Found(file_entry) => {
                    if let Some(FileContent::Inline(data)) = &file_entry.content {
                        assert_eq!(data, readme);
                    } else {
                        panic!("expected inline content");
                    }
                }
                other => panic!("expected Found for readme.md, got {other:?}"),
            }
        }
        other => panic!("expected Found for docs, got {other:?}"),
    }
}

#[tokio::test]
async fn test_two_clients_sync_via_head_poll() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let storage2 = connect_second_client(port);

    // Client 1 writes a file
    write_single_file(
        &storage,
        "main",
        "shared.txt",
        b"written by client-1",
        None,
        None,
        "client-1",
        "client-1 initial",
    )
    .await;

    // Client 2 polls HEAD and reads the file
    let (head, _) = storage2.get_head("main").await.unwrap().unwrap();
    assert_eq!(head.message, "client-1 initial");
    assert_eq!(head.client_id, "client-1");

    let data = read_file(&storage2, "main", "shared.txt").await;
    assert_eq!(data, b"written by client-1");
}

#[tokio::test]
async fn test_cas_conflict_between_clients() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let storage2 = connect_second_client(port);

    // Both clients see the initial state
    let (_, etag1) = write_single_file(
        &storage,
        "main",
        "file.txt",
        b"initial",
        None,
        None,
        "client-1",
        "initial",
    )
    .await;

    // Client 2 reads the current HEAD
    let (commit1, etag_from_client2) = storage2.get_head("main").await.unwrap().unwrap();
    let commit1_hash = commit1.hash().unwrap();

    // Client 1 updates first (succeeds)
    let blob_hash_hex = storage.put_blob(b"client-1 update").await.unwrap();
    let blob_hash = hash::hex_decode(&blob_hash_hex).unwrap();
    let root_a = make_leaf(vec![LeafEntry::file(
        "file.txt".to_string(),
        15,
        FileContent::Blocks(vec![blob_hash]),
        now_ns(),
        now_ns(),
        1000,
        1000,
        false,
    )]);
    let root_a_hex = storage.put_node(&root_a).await.unwrap();
    let root_a_hash = hash::hex_decode(&root_a_hex).unwrap();
    let commit_a = Commit::new(
        root_a_hash,
        Some(commit1_hash),
        "client-1".to_string(),
        now_ms(),
        "client-1 update".to_string(),
    );
    let _new_etag = storage
        .put_head("main", &commit_a, Some(&etag1))
        .await
        .unwrap();

    // Client 2 tries to update with stale etag - should get CAS conflict
    let blob2_hash_hex = storage2.put_blob(b"client-2 update").await.unwrap();
    let blob2_hash = hash::hex_decode(&blob2_hash_hex).unwrap();
    let root_b = make_leaf(vec![LeafEntry::file(
        "file.txt".to_string(),
        15,
        FileContent::Blocks(vec![blob2_hash]),
        now_ns(),
        now_ns(),
        1000,
        1000,
        false,
    )]);
    let root_b_hex = storage2.put_node(&root_b).await.unwrap();
    let root_b_hash = hash::hex_decode(&root_b_hex).unwrap();
    let commit_b = Commit::new(
        root_b_hash,
        Some(commit1_hash),
        "client-2".to_string(),
        now_ms(),
        "client-2 update".to_string(),
    );

    let result = storage2
        .put_head("main", &commit_b, Some(&etag_from_client2))
        .await;

    // CAS conflict - the stale etag no longer matches
    assert!(
        result.is_err(),
        "expected CAS conflict but got: {result:?}"
    );
}

#[tokio::test]
async fn test_cas_retry_after_conflict() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let storage2 = connect_second_client(port);

    // Initial commit
    let (_, etag1) = write_single_file(
        &storage,
        "main",
        "counter.txt",
        b"0",
        None,
        None,
        "client-1",
        "init",
    )
    .await;

    // Client 2 reads stale etag
    let (_commit1, stale_etag) = storage2.get_head("main").await.unwrap().unwrap();

    // Client 1 writes
    let (commit1, _) = storage.get_head("main").await.unwrap().unwrap();
    let commit1_hash = commit1.hash().unwrap();
    let blob_hex = storage.put_blob(b"1").await.unwrap();
    let blob_hash = hash::hex_decode(&blob_hex).unwrap();
    let root = make_leaf(vec![LeafEntry::file(
        "counter.txt".to_string(),
        1,
        FileContent::Blocks(vec![blob_hash]),
        now_ns(),
        now_ns(),
        1000,
        1000,
        false,
    )]);
    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();
    let commit_a = Commit::new(
        root_hash,
        Some(commit1_hash),
        "client-1".to_string(),
        now_ms(),
        "increment to 1".to_string(),
    );
    let _etag_a = storage
        .put_head("main", &commit_a, Some(&etag1))
        .await
        .unwrap();

    // Client 2 tries with stale etag - fails
    let blob2_hex = storage2.put_blob(b"2").await.unwrap();
    let blob2_hash = hash::hex_decode(&blob2_hex).unwrap();
    let root2 = make_leaf(vec![LeafEntry::file(
        "counter.txt".to_string(),
        1,
        FileContent::Blocks(vec![blob2_hash]),
        now_ns(),
        now_ns(),
        1000,
        1000,
        false,
    )]);
    let root2_hex = storage2.put_node(&root2).await.unwrap();
    let root2_hash = hash::hex_decode(&root2_hex).unwrap();

    let commit_b_attempt1 = Commit::new(
        root2_hash,
        Some(commit1_hash),
        "client-2".to_string(),
        now_ms(),
        "increment to 2".to_string(),
    );
    let result = storage2
        .put_head("main", &commit_b_attempt1, Some(&stale_etag))
        .await;
    assert!(result.is_err());

    // Client 2 retries: re-reads HEAD, gets fresh etag, succeeds
    let (fresh_commit, fresh_etag) = storage2.get_head("main").await.unwrap().unwrap();
    let fresh_commit_hash = fresh_commit.hash().unwrap();

    let commit_b_retry = Commit::new(
        root2_hash,
        Some(fresh_commit_hash),
        "client-2".to_string(),
        now_ms(),
        "increment to 2 (retry)".to_string(),
    );
    let etag_b = storage2
        .put_head("main", &commit_b_retry, Some(&fresh_etag))
        .await;
    assert!(etag_b.is_ok(), "retry should succeed with fresh etag");

    // Verify final state
    let data = read_file(&storage2, "main", "counter.txt").await;
    assert_eq!(data, b"2");
}

#[tokio::test]
async fn test_content_dedup_across_clients() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let storage2 = connect_second_client(port);

    let content = b"shared content blob";

    // Both clients upload the same content
    let hash1 = storage.put_blob(content).await.unwrap();
    let hash2 = storage2.put_blob(content).await.unwrap();
    assert_eq!(hash1, hash2, "same content should produce same hash");

    // Both can read it
    let data1 = storage.get_blob(&hash1).await.unwrap();
    let data2 = storage2.get_blob(&hash2).await.unwrap();
    assert_eq!(data1, content);
    assert_eq!(data2, content);
}

#[tokio::test]
async fn test_multi_file_directory() {
    let (storage, _container) = start_rustfs_drive().await;

    // Write multiple blobs
    let files: Vec<(&str, &[u8])> = vec![
        ("alpha.txt", b"aaa"),
        ("beta.txt", b"bbb"),
        ("gamma.txt", b"ggg"),
    ];

    let mut entries = Vec::new();
    for (name, content) in &files {
        let blob_hex = storage.put_blob(*content).await.unwrap();
        let blob_hash = hash::hex_decode(&blob_hex).unwrap();
        entries.push(LeafEntry::file(
            name.to_string(),
            content.len() as u64,
            FileContent::Blocks(vec![blob_hash]),
            now_ns(),
            now_ns(),
            1000,
            1000,
            false,
        ));
    }

    let root = make_leaf(entries);
    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();

    let commit = Commit::new(
        root_hash,
        None,
        "client-1".to_string(),
        now_ms(),
        "multi-file".to_string(),
    );
    storage.put_head("main", &commit, None).await.unwrap();

    // Read each file back
    for (name, expected) in &files {
        let data = read_file(&storage, "main", name).await;
        assert_eq!(&data, *expected, "mismatch for {name}");
    }
}

#[tokio::test]
async fn test_multiple_branches_independent() {
    let (storage, _container) = start_rustfs_drive().await;

    // Write to "main"
    write_single_file(
        &storage,
        "main",
        "file.txt",
        b"main content",
        None,
        None,
        "client-1",
        "main commit",
    )
    .await;

    // Write to "dev"
    write_single_file(
        &storage,
        "dev",
        "file.txt",
        b"dev content",
        None,
        None,
        "client-1",
        "dev commit",
    )
    .await;

    // Each branch has its own content
    let main_data = read_file(&storage, "main", "file.txt").await;
    let dev_data = read_file(&storage, "dev", "file.txt").await;

    assert_eq!(main_data, b"main content");
    assert_eq!(dev_data, b"dev content");

    // Branch heads are independent
    let (main_head, _) = storage.get_head("main").await.unwrap().unwrap();
    let (dev_head, _) = storage.get_head("dev").await.unwrap().unwrap();
    assert_eq!(main_head.message, "main commit");
    assert_eq!(dev_head.message, "dev commit");
}

#[tokio::test]
async fn test_large_file_multiple_blocks() {
    let (storage, _container) = start_rustfs_drive().await;

    // Simulate a large file split into multiple blocks
    let block1 = vec![0xAA_u8; 4096];
    let block2 = vec![0xBB_u8; 4096];
    let block3 = vec![0xCC_u8; 2048];

    let h1 = storage.put_blob(&block1).await.unwrap();
    let h2 = storage.put_blob(&block2).await.unwrap();
    let h3 = storage.put_blob(&block3).await.unwrap();

    let total_size = block1.len() + block2.len() + block3.len();
    let root = make_leaf(vec![LeafEntry::file(
        "bigfile.bin".to_string(),
        total_size as u64,
        FileContent::Blocks(vec![
            hash::hex_decode(&h1).unwrap(),
            hash::hex_decode(&h2).unwrap(),
            hash::hex_decode(&h3).unwrap(),
        ]),
        now_ns(),
        now_ns(),
        1000,
        1000,
        false,
    )]);

    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();

    let commit = Commit::new(
        root_hash,
        None,
        "client-1".to_string(),
        now_ms(),
        "large file".to_string(),
    );
    storage.put_head("main", &commit, None).await.unwrap();

    // Read back all blocks and reassemble
    let reassembled = read_file(&storage, "main", "bigfile.bin").await;
    assert_eq!(reassembled.len(), total_size);
    assert_eq!(&reassembled[..4096], &block1[..]);
    assert_eq!(&reassembled[4096..8192], &block2[..]);
    assert_eq!(&reassembled[8192..], &block3[..]);
}

#[tokio::test]
async fn test_symlink_and_executable() {
    let (storage, _container) = start_rustfs_drive().await;

    let root = make_leaf(vec![
        LeafEntry::file(
            "run.sh".to_string(),
            10,
            FileContent::Inline(b"#!/bin/sh\n".to_vec()),
            now_ns(),
            now_ns(),
            0,
            0,
            true, // executable
        ),
        LeafEntry::symlink(
            "start".to_string(),
            "run.sh".to_string(),
            now_ns(),
            now_ns(),
            0,
            0,
        ),
    ]);

    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();

    let commit = Commit::new(
        root_hash,
        None,
        "client-1".to_string(),
        now_ms(),
        "exec and symlink".to_string(),
    );
    storage.put_head("main", &commit, None).await.unwrap();

    // Read back and verify attributes
    let (head, _) = storage.get_head("main").await.unwrap().unwrap();
    let root_node = storage.get_node(&head.root).await.unwrap();

    match root_node.lookup_local("run.sh") {
        LookupResult::Found(entry) => {
            assert!(entry.exec, "run.sh should be executable");
            assert_eq!(entry.entry_type, EntryType::File);
        }
        other => panic!("expected Found, got {other:?}"),
    }

    match root_node.lookup_local("start") {
        LookupResult::Found(entry) => {
            assert_eq!(entry.entry_type, EntryType::Symlink);
            assert_eq!(entry.target.as_deref(), Some("run.sh"));
        }
        other => panic!("expected Found, got {other:?}"),
    }
}

#[tokio::test]
async fn test_sequential_commits_history() {
    let (storage, _container) = start_rustfs_drive().await;

    // Build a chain of 5 commits
    let mut parent_hash: Option<[u8; 32]> = None;
    let mut etag: Option<String> = None;

    for i in 0..5 {
        let content = format!("version {i}");
        let blob_hex = storage.put_blob(content.as_bytes()).await.unwrap();
        let blob_hash = hash::hex_decode(&blob_hex).unwrap();

        let root = make_leaf(vec![LeafEntry::file(
            "evolving.txt".to_string(),
            content.len() as u64,
            FileContent::Blocks(vec![blob_hash]),
            now_ns(),
            now_ns(),
            1000,
            1000,
            false,
        )]);
        let root_hex = storage.put_node(&root).await.unwrap();
        let root_hash = hash::hex_decode(&root_hex).unwrap();

        let commit = Commit::new(
            root_hash,
            parent_hash,
            "client-1".to_string(),
            now_ms() + i,
            format!("commit {i}"),
        );
        let commit_hash = commit.hash().unwrap();

        let new_etag = storage
            .put_head("main", &commit, etag.as_deref())
            .await
            .unwrap();

        parent_hash = Some(commit_hash);
        etag = Some(new_etag);
    }

    // Verify the latest commit
    let (head, _) = storage.get_head("main").await.unwrap().unwrap();
    assert_eq!(head.message, "commit 4");
    assert!(head.parent.is_some(), "last commit should have a parent");

    // Verify latest file content
    let data = read_file(&storage, "main", "evolving.txt").await;
    assert_eq!(data, b"version 4");
}

#[tokio::test]
async fn test_client2_sees_updates_after_poll() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let storage2 = connect_second_client(port);

    // Client 2 sees no HEAD initially
    let head = storage2.get_head("main").await.unwrap();
    assert!(head.is_none());

    // Client 1 writes
    write_single_file(
        &storage,
        "main",
        "new.txt",
        b"first write",
        None,
        None,
        "client-1",
        "first",
    )
    .await;

    // Client 2 polls and sees the update
    let (head, etag1) = storage2.get_head("main").await.unwrap().unwrap();
    assert_eq!(head.message, "first");
    let data = read_file(&storage2, "main", "new.txt").await;
    assert_eq!(data, b"first write");

    // Client 1 writes again
    let commit1_hash = head.hash().unwrap();
    write_single_file(
        &storage,
        "main",
        "new.txt",
        b"second write",
        Some(commit1_hash),
        // Need to get client 1's etag, so read from storage
        {
            let (_, e) = storage.get_head("main").await.unwrap().unwrap();
            Some(e)
        }
        .as_deref(),
        "client-1",
        "second",
    )
    .await;

    // Client 2 polls again
    let (head2, etag2) = storage2.get_head("main").await.unwrap().unwrap();
    assert_eq!(head2.message, "second");
    assert_ne!(etag1, etag2, "etag should change after update");

    let data2 = read_file(&storage2, "main", "new.txt").await;
    assert_eq!(data2, b"second write");
}

#[tokio::test]
async fn test_blob_exists_check() {
    let (storage, _container) = start_rustfs_drive().await;

    let hex = storage.put_blob(b"existence check").await.unwrap();
    assert!(storage.blob_exists(&hex).await.unwrap());

    // A hash that was never stored
    let fake = hash::hex_encode(&[0xFF; 32]);
    assert!(!storage.blob_exists(&fake).await.unwrap());
}

#[tokio::test]
async fn test_node_exists_check() {
    let (storage, _container) = start_rustfs_drive().await;

    let node = make_leaf(vec![inline_file("test.txt", b"data")]);
    let hex = storage.put_node(&node).await.unwrap();
    assert!(storage.node_exists(&hex).await.unwrap());

    let fake = hash::hex_encode(&[0xFF; 32]);
    assert!(!storage.node_exists(&fake).await.unwrap());
}

#[tokio::test]
async fn test_concurrent_writes_to_separate_branches() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let storage2 = connect_second_client(port);

    let storage = Arc::new(storage);
    let storage2 = Arc::new(storage2);

    // Both clients write to different branches concurrently
    let s1 = storage.clone();
    let h1 = tokio::spawn(async move {
        let blob_hex = s1.put_blob(b"branch-a data").await.unwrap();
        let blob_hash = hash::hex_decode(&blob_hex).unwrap();
        let root = make_leaf(vec![LeafEntry::file(
            "file.txt".to_string(),
            13,
            FileContent::Blocks(vec![blob_hash]),
            now_ns(),
            now_ns(),
            1000,
            1000,
            false,
        )]);
        let root_hex = s1.put_node(&root).await.unwrap();
        let root_hash = hash::hex_decode(&root_hex).unwrap();
        let commit = Commit::new(
            root_hash,
            None,
            "client-1".to_string(),
            now_ms(),
            "branch-a".to_string(),
        );
        s1.put_head("branch-a", &commit, None).await.unwrap();
    });

    let s2 = storage2.clone();
    let h2 = tokio::spawn(async move {
        let blob_hex = s2.put_blob(b"branch-b data").await.unwrap();
        let blob_hash = hash::hex_decode(&blob_hex).unwrap();
        let root = make_leaf(vec![LeafEntry::file(
            "file.txt".to_string(),
            13,
            FileContent::Blocks(vec![blob_hash]),
            now_ns(),
            now_ns(),
            1000,
            1000,
            false,
        )]);
        let root_hex = s2.put_node(&root).await.unwrap();
        let root_hash = hash::hex_decode(&root_hex).unwrap();
        let commit = Commit::new(
            root_hash,
            None,
            "client-2".to_string(),
            now_ms(),
            "branch-b".to_string(),
        );
        s2.put_head("branch-b", &commit, None).await.unwrap();
    });

    h1.await.unwrap();
    h2.await.unwrap();

    // Each branch has its own independent data
    let a_data = read_file(storage.as_ref(), "branch-a", "file.txt").await;
    let b_data = read_file(storage2.as_ref(), "branch-b", "file.txt").await;
    assert_eq!(a_data, b"branch-a data");
    assert_eq!(b_data, b"branch-b data");
}

#[tokio::test]
async fn test_immutable_snapshots_preserved() {
    let (storage, _container) = start_rustfs_drive().await;

    // Commit v1
    let blob1_hex = storage.put_blob(b"snapshot v1").await.unwrap();
    let root1 = make_leaf(vec![LeafEntry::file(
        "data.txt".to_string(),
        11,
        FileContent::Blocks(vec![hash::hex_decode(&blob1_hex).unwrap()]),
        now_ns(),
        now_ns(),
        1000,
        1000,
        false,
    )]);
    let root1_hex = storage.put_node(&root1).await.unwrap();
    let root1_hash = hash::hex_decode(&root1_hex).unwrap();
    let commit1 = Commit::new(
        root1_hash,
        None,
        "client-1".to_string(),
        now_ms(),
        "v1".to_string(),
    );
    let etag1 = storage.put_head("main", &commit1, None).await.unwrap();

    // Commit v2 (overwrites HEAD)
    let blob2_hex = storage.put_blob(b"snapshot v2").await.unwrap();
    let root2 = make_leaf(vec![LeafEntry::file(
        "data.txt".to_string(),
        11,
        FileContent::Blocks(vec![hash::hex_decode(&blob2_hex).unwrap()]),
        now_ns(),
        now_ns(),
        1000,
        1000,
        false,
    )]);
    let root2_hex = storage.put_node(&root2).await.unwrap();
    let root2_hash = hash::hex_decode(&root2_hex).unwrap();
    let commit2 = Commit::new(
        root2_hash,
        Some(commit1.hash().unwrap()),
        "client-1".to_string(),
        now_ms(),
        "v2".to_string(),
    );
    storage
        .put_head("main", &commit2, Some(&etag1))
        .await
        .unwrap();

    // HEAD points to v2
    let data_head = read_file(&storage, "main", "data.txt").await;
    assert_eq!(data_head, b"snapshot v2");

    // But v1's tree and blob are still accessible (content-addressed, immutable)
    let old_root = storage.get_node(&root1_hex).await.unwrap();
    match old_root.lookup_local("data.txt") {
        LookupResult::Found(entry) => {
            if let Some(FileContent::Blocks(blocks)) = &entry.content {
                let old_data = storage
                    .get_blob(&hash::hex_encode(&blocks[0]))
                    .await
                    .unwrap();
                assert_eq!(old_data, b"snapshot v1");
            }
        }
        _ => panic!("old snapshot should still be readable"),
    }
}
