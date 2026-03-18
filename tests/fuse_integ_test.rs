//! Integration tests that mount a merkle-drive FUSE filesystem backed by a real
//! RustFS (S3-compatible) endpoint running in Docker via testcontainers.
//!
//! These tests exercise the FUSE interface: writing and reading files through
//! standard filesystem operations, then verifying that data persists correctly
//! in the S3-backed storage.

use std::os::unix::fs::symlink as unix_symlink;
use std::path::PathBuf;
use std::time::Duration;

use merkle_drive::commit::Commit;
use merkle_drive::fuse::MerkleFuse;
use merkle_drive::hash;
use merkle_drive::storage::{S3Storage, Storage};
use merkle_drive::tree::*;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const DATA_BUCKET: &str = "merkle-data";
const META_BUCKET: &str = "merkle-meta";

// ============================================================
// Container / client helpers (shared with rustfs_integ_test.rs)
// ============================================================

async fn start_rustfs_drive() -> (S3Storage, testcontainers::ContainerAsync<GenericImage>) {
    let container = GenericImage::new("rustfs/rustfs", "latest")
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Starting:"))
        .with_env_var("RUSTFS_ACCESS_KEY", "testuser")
        .with_env_var("RUSTFS_SECRET_KEY", "testpassword")
        .start()
        .await
        .expect("failed to start RustFS container");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("failed to get mapped port");

    let endpoint = format!("http://127.0.0.1:{port}");

    let creds =
        aws_credential_types::Credentials::new("testuser", "testpassword", None, None, "test");

    let config = aws_sdk_s3::Config::builder()
        .endpoint_url(&endpoint)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(creds)
        .force_path_style(true)
        .behavior_version_latest()
        .build();

    let client = aws_sdk_s3::Client::from_conf(config);

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

    let storage = S3Storage::new(client, DATA_BUCKET.to_string(), META_BUCKET.to_string());

    (storage, container)
}

fn connect_second_client(port: u16) -> S3Storage {
    let endpoint = format!("http://127.0.0.1:{port}");

    let creds =
        aws_credential_types::Credentials::new("testuser", "testpassword", None, None, "test");

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
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn make_leaf(entries: Vec<LeafEntry>) -> TreeNode {
    TreeNode::Leaf(LeafNode { entries })
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

/// Navigate into a subdirectory and read a file from it.
async fn read_nested_file(
    storage: &S3Storage,
    branch: &str,
    dir_name: &str,
    filename: &str,
) -> Vec<u8> {
    let (commit, _) = storage
        .get_head(branch)
        .await
        .unwrap()
        .expect("branch HEAD should exist");

    let root = storage.get_node(&commit.root).await.unwrap();

    match root.lookup_local(dir_name) {
        LookupResult::Found(entry) => {
            assert_eq!(entry.entry_type, EntryType::Dir);
            let dir_node = storage
                .get_node(&hash::hex_encode(&entry.hash.unwrap()))
                .await
                .unwrap();

            match dir_node.lookup_local(filename) {
                LookupResult::Found(file_entry) => match &file_entry.content {
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
        other => panic!("expected Found for {dir_name}, got {other:?}"),
    }
}

// ============================================================
// FUSE mount helper
// ============================================================

struct FuseMount {
    session: Option<fuser::BackgroundSession>,
    mount_dir: tempfile::TempDir,
}

impl FuseMount {
    async fn new(storage: S3Storage, branch: &str) -> Self {
        let mount_dir = tempfile::tempdir().expect("failed to create temp dir");
        let rt = tokio::runtime::Handle::current();
        let fs = MerkleFuse::new(storage, rt, branch.to_string(), "fuse-test".to_string()).await;
        let session = fs
            .mount(mount_dir.path())
            .expect("failed to mount FUSE filesystem");

        // Give FUSE time to be ready
        tokio::time::sleep(Duration::from_millis(500)).await;

        Self {
            session: Some(session),
            mount_dir,
        }
    }

    fn path(&self) -> PathBuf {
        self.mount_dir.path().to_owned()
    }

    fn unmount(&mut self) {
        self.session.take(); // Drop the session to unmount
    }
}

impl Drop for FuseMount {
    fn drop(&mut self) {
        self.session.take();
    }
}

/// Run a blocking filesystem operation on a separate thread to avoid
/// blocking the tokio runtime.
async fn blocking<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.unwrap()
}

// ============================================================
// Integration tests
// ============================================================

#[tokio::test]
async fn test_fuse_write_and_read_file() {
    let (storage, _container) = start_rustfs_drive().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Write a file via FUSE
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("hello.txt"), b"hello from FUSE!").unwrap();
    })
    .await;

    // Read it back via FUSE
    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("hello.txt")).unwrap()).await;

    assert_eq!(content, b"hello from FUSE!");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_data_persists_to_storage() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Write a file via FUSE
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("persisted.txt"), b"this should persist").unwrap();
    })
    .await;

    // Unmount (triggers commit via destroy)
    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify via library using a second client
    let storage2 = connect_second_client(port);
    let data = read_file(&storage2, "main", "persisted.txt").await;
    assert_eq!(data, b"this should persist");
}

#[tokio::test]
async fn test_fuse_read_preexisting_data() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();

    // Write data via library API
    let blob_hex = storage.put_blob(b"pre-existing content").await.unwrap();
    let blob_hash = hash::hex_decode(&blob_hex).unwrap();
    let root = make_leaf(vec![LeafEntry::file(
        "existing.txt".to_string(),
        20,
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
        None,
        "library-client".to_string(),
        now_ms(),
        "pre-seed".to_string(),
    );
    storage.put_head("main", &commit, None).await.unwrap();

    // Mount FUSE with a fresh client (to prove it loads from storage)
    let storage2 = connect_second_client(port);
    let mut mount = FuseMount::new(storage2, "main").await;
    let mp = mount.path();

    // Read via FUSE
    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("existing.txt")).unwrap()).await;

    assert_eq!(content, b"pre-existing content");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_multiple_files() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Write multiple files
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("alpha.txt"), b"aaa").unwrap();
        std::fs::write(mp2.join("beta.txt"), b"bbb").unwrap();
        std::fs::write(mp2.join("gamma.txt"), b"ggg").unwrap();
    })
    .await;

    // Read them back via FUSE
    let mp2 = mp.clone();
    let contents = blocking(move || {
        vec![
            std::fs::read(mp2.join("alpha.txt")).unwrap(),
            std::fs::read(mp2.join("beta.txt")).unwrap(),
            std::fs::read(mp2.join("gamma.txt")).unwrap(),
        ]
    })
    .await;

    assert_eq!(contents[0], b"aaa");
    assert_eq!(contents[1], b"bbb");
    assert_eq!(contents[2], b"ggg");

    // List directory
    let mp2 = mp.clone();
    let mut names: Vec<String> = blocking(move || {
        std::fs::read_dir(&mp2)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_str().unwrap().to_string())
            .collect()
    })
    .await;
    names.sort();
    assert_eq!(names, vec!["alpha.txt", "beta.txt", "gamma.txt"]);

    // Unmount and verify persistence
    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let storage2 = connect_second_client(port);
    let a = read_file(&storage2, "main", "alpha.txt").await;
    let b = read_file(&storage2, "main", "beta.txt").await;
    let g = read_file(&storage2, "main", "gamma.txt").await;
    assert_eq!(a, b"aaa");
    assert_eq!(b, b"bbb");
    assert_eq!(g, b"ggg");
}

#[tokio::test]
async fn test_fuse_nested_directories() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Create nested directory structure: src/main.rs, docs/readme.md
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("src")).unwrap();
        std::fs::create_dir(mp2.join("docs")).unwrap();
        std::fs::write(mp2.join("src").join("main.rs"), b"fn main() {}").unwrap();
        std::fs::write(mp2.join("docs").join("readme.md"), b"# Docs").unwrap();
        std::fs::write(mp2.join("Cargo.toml"), b"[package]").unwrap();
    })
    .await;

    // Read back via FUSE
    let mp2 = mp.clone();
    let results = blocking(move || {
        (
            std::fs::read(mp2.join("src").join("main.rs")).unwrap(),
            std::fs::read(mp2.join("docs").join("readme.md")).unwrap(),
            std::fs::read(mp2.join("Cargo.toml")).unwrap(),
        )
    })
    .await;

    assert_eq!(results.0, b"fn main() {}");
    assert_eq!(results.1, b"# Docs");
    assert_eq!(results.2, b"[package]");

    // Unmount and verify persistence
    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let storage2 = connect_second_client(port);
    let main_rs = read_nested_file(&storage2, "main", "src", "main.rs").await;
    let readme = read_nested_file(&storage2, "main", "docs", "readme.md").await;
    let cargo = read_file(&storage2, "main", "Cargo.toml").await;

    assert_eq!(main_rs, b"fn main() {}");
    assert_eq!(readme, b"# Docs");
    assert_eq!(cargo, b"[package]");
}

#[tokio::test]
async fn test_fuse_overwrite_file() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Write v1
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("data.txt"), b"version 1").unwrap();
    })
    .await;

    // Read v1
    let mp2 = mp.clone();
    let v1 = blocking(move || std::fs::read(mp2.join("data.txt")).unwrap()).await;
    assert_eq!(v1, b"version 1");

    // Overwrite with v2
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("data.txt"), b"version 2").unwrap();
    })
    .await;

    // Read v2
    let mp2 = mp.clone();
    let v2 = blocking(move || std::fs::read(mp2.join("data.txt")).unwrap()).await;
    assert_eq!(v2, b"version 2");

    // Unmount and verify final version persisted
    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let storage2 = connect_second_client(port);
    let data = read_file(&storage2, "main", "data.txt").await;
    assert_eq!(data, b"version 2");
}

#[tokio::test]
async fn test_fuse_delete_file() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Create and then delete a file
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("temp.txt"), b"temporary").unwrap();
        std::fs::write(mp2.join("keep.txt"), b"permanent").unwrap();
        std::fs::remove_file(mp2.join("temp.txt")).unwrap();
    })
    .await;

    // Verify temp.txt is gone, keep.txt remains
    let mp2 = mp.clone();
    let results = blocking(move || {
        (
            mp2.join("temp.txt").exists(),
            std::fs::read(mp2.join("keep.txt")).unwrap(),
        )
    })
    .await;

    assert!(!results.0, "temp.txt should be deleted");
    assert_eq!(results.1, b"permanent");

    // Unmount and verify storage only has keep.txt
    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let storage2 = connect_second_client(port);
    let (commit, _) = storage2.get_head("main").await.unwrap().unwrap();
    let root = storage2.get_node(&commit.root).await.unwrap();

    match root.lookup_local("temp.txt") {
        LookupResult::NotFound => {} // expected
        other => panic!("expected NotFound for temp.txt, got {other:?}"),
    }
    match root.lookup_local("keep.txt") {
        LookupResult::Found(entry) => {
            assert_eq!(entry.entry_type, EntryType::File);
        }
        other => panic!("expected Found for keep.txt, got {other:?}"),
    }
}

#[tokio::test]
async fn test_fuse_symlink() {
    let (storage, _container) = start_rustfs_drive().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("target.txt"), b"target content").unwrap();
        unix_symlink("target.txt", mp2.join("link.txt")).unwrap();
    })
    .await;

    // Read the symlink target
    let mp2 = mp.clone();
    let (link_target, content) = blocking(move || {
        let target = std::fs::read_link(mp2.join("link.txt")).unwrap();
        let content = std::fs::read(mp2.join("link.txt")).unwrap();
        (target, content)
    })
    .await;

    assert_eq!(link_target, std::path::PathBuf::from("target.txt"));
    assert_eq!(content, b"target content");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_large_file() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Write a file larger than the inline threshold (128 bytes)
    let large_content: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
    let expected = large_content.clone();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("large.bin"), &large_content).unwrap();
    })
    .await;

    // Read back via FUSE
    let mp2 = mp.clone();
    let read_back = blocking(move || std::fs::read(mp2.join("large.bin")).unwrap()).await;
    assert_eq!(read_back, expected);

    // Unmount and verify via library
    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let storage2 = connect_second_client(port);
    let data = read_file(&storage2, "main", "large.bin").await;
    assert_eq!(data, expected);
}

#[tokio::test]
async fn test_fuse_rmdir() {
    let (storage, _container) = start_rustfs_drive().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("subdir")).unwrap();
        std::fs::write(mp2.join("subdir").join("file.txt"), b"inside").unwrap();
        // Remove file first, then directory
        std::fs::remove_file(mp2.join("subdir").join("file.txt")).unwrap();
        std::fs::remove_dir(mp2.join("subdir")).unwrap();
    })
    .await;

    // Verify directory is gone
    let mp2 = mp.clone();
    let exists = blocking(move || mp2.join("subdir").exists()).await;
    assert!(!exists, "subdir should be removed");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_read_preexisting_nested() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();

    // Build a nested tree via library API: root -> src/ -> lib.rs
    let lib_content = b"pub fn hello() {}";
    let blob_hex = storage.put_blob(lib_content).await.unwrap();
    let blob_hash = hash::hex_decode(&blob_hex).unwrap();

    let src_dir = make_leaf(vec![LeafEntry::file(
        "lib.rs".to_string(),
        lib_content.len() as u64,
        FileContent::Blocks(vec![blob_hash]),
        now_ns(),
        now_ns(),
        1000,
        1000,
        false,
    )]);
    let src_hex = storage.put_node(&src_dir).await.unwrap();
    let src_hash = hash::hex_decode(&src_hex).unwrap();

    let root = make_leaf(vec![
        LeafEntry::file(
            "README.md".to_string(),
            7,
            FileContent::Inline(b"# Hello".to_vec()),
            now_ns(),
            now_ns(),
            1000,
            1000,
            false,
        ),
        LeafEntry::dir("src".to_string(), src_hash),
    ]);
    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();

    let commit = Commit::new(
        root_hash,
        None,
        "library-client".to_string(),
        now_ms(),
        "nested seed".to_string(),
    );
    storage.put_head("main", &commit, None).await.unwrap();

    // Mount with a fresh client and read via FUSE
    let storage2 = connect_second_client(port);
    let mut mount = FuseMount::new(storage2, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    let results = blocking(move || {
        (
            std::fs::read(mp2.join("README.md")).unwrap(),
            std::fs::read(mp2.join("src").join("lib.rs")).unwrap(),
        )
    })
    .await;

    assert_eq!(results.0, b"# Hello");
    assert_eq!(results.1, b"pub fn hello() {}");

    mount.unmount();
}
