//! Concurrency integration tests for merkle-drive.
//!
//! Tests CAS conflict handling, concurrent writes to different branches,
//! and concurrent reads during writes.

mod common;

use std::sync::Arc;

use merkle_drive::commit::Commit;
use merkle_drive::hash;
use merkle_drive::tree::*;

use common::*;

#[tokio::test]
async fn test_concurrent_cas_race() {
    let container = RustfsContainer::start().await;
    let (storage1, storage2) = container.new_storage_pair().await;

    // Initial commit by client 1
    let (_, etag1) = write_single_file(
        &storage1, "main", "file.txt", b"initial", None, None, "client-1", "init",
    )
    .await;

    // Client 2 reads current HEAD (stale after client 1 updates)
    let (commit1, stale_etag) = storage2.get_head("main").await.unwrap().unwrap();
    let commit1_hash = commit1.hash().unwrap();

    // Client 1 updates first
    let (_, _new_etag) = write_single_file(
        &storage1,
        "main",
        "file.txt",
        b"client-1 update",
        Some(commit1_hash),
        Some(&etag1),
        "client-1",
        "update by 1",
    )
    .await;

    // Client 2 tries with stale etag - should fail
    let blob2_hex = storage2.put_blob(b"client-2 update").await.unwrap();
    let blob2_hash = hash::hex_decode(&blob2_hex).unwrap();
    let root2 = make_leaf(vec![LeafEntry::file(
        "file.txt".to_string(),
        15,
        FileContent::Blocks(vec![blob2_hash]),
        now_ns(),
        now_ns(),
        1000,
        1000,
        false,
    )]);
    let root2_hex = storage2.put_node(&root2).await.unwrap();
    let root2_hash = hash::hex_decode(&root2_hex).unwrap();
    let commit_b = Commit::new(
        root2_hash,
        Some(commit1_hash),
        "client-2".to_string(),
        now_ms(),
        "client-2 update".to_string(),
    );

    let result = storage2
        .put_head("main", &commit_b, Some(&stale_etag))
        .await;
    assert!(result.is_err(), "expected CAS conflict but got: {result:?}");

    // Client 2 retries with fresh etag
    let (fresh_commit, fresh_etag) = storage2.get_head("main").await.unwrap().unwrap();
    let fresh_hash = fresh_commit.hash().unwrap();

    let commit_b_retry = Commit::new(
        root2_hash,
        Some(fresh_hash),
        "client-2".to_string(),
        now_ms(),
        "client-2 retry".to_string(),
    );
    let result = storage2
        .put_head("main", &commit_b_retry, Some(&fresh_etag))
        .await;
    assert!(result.is_ok(), "retry should succeed with fresh etag");

    // Verify final state
    let data = read_file(&storage2, "main", "file.txt").await;
    assert_eq!(data, b"client-2 update");
}

#[tokio::test]
async fn test_concurrent_writes_different_branches() {
    let container = RustfsContainer::start().await;
    let (storage1, storage2) = container.new_storage_pair().await;

    let s1 = Arc::new(storage1);
    let s2 = Arc::new(storage2);

    // Both clients write to different branches concurrently
    let s1c = s1.clone();
    let h1 = tokio::spawn(async move {
        let blob_hex = s1c.put_blob(b"branch-a data").await.unwrap();
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
        let root_hex = s1c.put_node(&root).await.unwrap();
        let root_hash = hash::hex_decode(&root_hex).unwrap();
        let commit = Commit::new(
            root_hash,
            None,
            "client-1".to_string(),
            now_ms(),
            "branch-a".to_string(),
        );
        s1c.put_head("branch-a", &commit, None).await.unwrap();
    });

    let s2c = s2.clone();
    let h2 = tokio::spawn(async move {
        let blob_hex = s2c.put_blob(b"branch-b data").await.unwrap();
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
        let root_hex = s2c.put_node(&root).await.unwrap();
        let root_hash = hash::hex_decode(&root_hex).unwrap();
        let commit = Commit::new(
            root_hash,
            None,
            "client-2".to_string(),
            now_ms(),
            "branch-b".to_string(),
        );
        s2c.put_head("branch-b", &commit, None).await.unwrap();
    });

    h1.await.unwrap();
    h2.await.unwrap();

    // Each branch has its own independent data
    let a_data = read_file(&s1, "branch-a", "file.txt").await;
    let b_data = read_file(&s2, "branch-b", "file.txt").await;
    assert_eq!(a_data, b"branch-a data");
    assert_eq!(b_data, b"branch-b data");
}

#[tokio::test]
async fn test_concurrent_reads_while_writing() {
    let container = RustfsContainer::start().await;
    let (storage1, storage2) = container.new_storage_pair().await;

    // Initial commit
    write_single_file(
        &storage1, "main", "file.txt", b"v0", None, None, "client-1", "init",
    )
    .await;

    let s1 = Arc::new(storage1);
    let s2 = Arc::new(storage2);

    // Writer task: makes 5 sequential commits
    let writer = {
        let s = s1.clone();
        tokio::spawn(async move {
            for i in 1..=5 {
                let content = format!("v{i}");
                let (head, etag) = s.get_head("main").await.unwrap().unwrap();
                let parent = head.hash().unwrap();
                write_single_file(
                    &s,
                    "main",
                    "file.txt",
                    content.as_bytes(),
                    Some(parent),
                    Some(&etag),
                    "client-1",
                    &format!("commit {i}"),
                )
                .await;
            }
        })
    };

    // Reader task: polls HEAD repeatedly, should always see consistent data
    let reader = {
        let s = s2.clone();
        tokio::spawn(async move {
            let mut read_count = 0;
            for _ in 0..20 {
                if let Ok(Some(_)) = s.get_head("main").await {
                    let data = read_file(&s, "main", "file.txt").await;
                    // Data should start with "v" and be a valid version
                    assert!(data.starts_with(b"v"), "unexpected data: {:?}", data);
                    read_count += 1;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert!(read_count > 0, "should have read at least once");
        })
    };

    writer.await.unwrap();
    reader.await.unwrap();

    // Final state should be v5
    let data = read_file(&s1, "main", "file.txt").await;
    assert_eq!(data, b"v5");
}

#[tokio::test]
async fn test_many_concurrent_blob_puts() {
    let container = RustfsContainer::start().await;
    let storage = Arc::new(container.new_storage().await);

    // 50 concurrent blob puts with different data
    let mut handles = Vec::new();
    for i in 0..50 {
        let s = storage.clone();
        handles.push(tokio::spawn(async move {
            let data = format!("blob-content-{i}");
            let hex = s.put_blob(data.as_bytes()).await.unwrap();
            (hex, data)
        }));
    }

    // Collect results and verify all blobs are readable
    for handle in handles {
        let (hex, expected) = handle.await.unwrap();
        let data = storage.get_blob(&hex).await.unwrap();
        assert_eq!(data, expected.as_bytes());
    }
}

#[tokio::test]
async fn test_cas_retry_loop() {
    let container = RustfsContainer::start().await;
    let (storage1, storage2) = container.new_storage_pair().await;

    // Initial commit
    write_single_file(
        &storage1, "main", "counter.txt", b"0", None, None, "client-1", "init",
    )
    .await;

    // Client 1 makes 3 rapid updates
    for i in 1..=3 {
        let (head, etag) = storage1.get_head("main").await.unwrap().unwrap();
        let parent = head.hash().unwrap();
        let content = format!("{i}");
        write_single_file(
            &storage1,
            "main",
            "counter.txt",
            content.as_bytes(),
            Some(parent),
            Some(&etag),
            "client-1",
            &format!("increment to {i}"),
        )
        .await;
    }

    // Client 2 tries to write with a stale etag, retries in a loop
    let mut attempts = 0;
    let max_attempts = 5;
    let blob_hex = storage2.put_blob(b"client-2-value").await.unwrap();
    let blob_hash = hash::hex_decode(&blob_hex).unwrap();

    loop {
        attempts += 1;
        assert!(attempts <= max_attempts, "too many CAS retries");

        let (head, etag) = storage2.get_head("main").await.unwrap().unwrap();
        let parent = head.hash().unwrap();

        let root = make_leaf(vec![LeafEntry::file(
            "counter.txt".to_string(),
            14,
            FileContent::Blocks(vec![blob_hash]),
            now_ns(),
            now_ns(),
            1000,
            1000,
            false,
        )]);
        let root_hex = storage2.put_node(&root).await.unwrap();
        let root_hash = hash::hex_decode(&root_hex).unwrap();

        let commit = Commit::new(
            root_hash,
            Some(parent),
            "client-2".to_string(),
            now_ms(),
            "client-2 write".to_string(),
        );

        match storage2.put_head("main", &commit, Some(&etag)).await {
            Ok(_) => break,
            Err(_) => continue, // CAS conflict, retry
        }
    }

    // Verify client-2's write landed
    let data = read_file(&storage2, "main", "counter.txt").await;
    assert_eq!(data, b"client-2-value");
}
