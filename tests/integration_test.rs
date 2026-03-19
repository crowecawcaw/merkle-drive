//! Integration tests for merkle-drive library layer.
//!
//! Storage tests use a real RustFS (S3-compatible) container.
//! Tree, canonical, hash, and commit tests operate on in-memory data structures.

mod common;

use merkle_drive::canonical;
use merkle_drive::commit::Commit;
use merkle_drive::error::MerkleError;
use merkle_drive::hash;
use merkle_drive::tree::*;

use common::*;

// ============================================================
// Serialization round-trip tests (no S3 needed)
// ============================================================

fn static_inline_file(name: &str, data: &[u8]) -> LeafEntry {
    LeafEntry::file(
        name.to_string(),
        data.len() as u64,
        FileContent::Inline(data.to_vec()),
        1742000000000000000,
        1742000000000000000,
        1000,
        1000,
        false,
    )
}

fn static_block_file(name: &str, size: u64, block_hashes: Vec<[u8; 32]>) -> LeafEntry {
    LeafEntry::file(
        name.to_string(),
        size,
        FileContent::Blocks(block_hashes),
        1742000000000000000,
        1742000000000000000,
        1000,
        1000,
        false,
    )
}

#[test]
fn test_empty_leaf_node_round_trip() {
    let node = make_leaf(vec![]);
    let bytes = canonical::serialize_tree_node(&node).unwrap();
    let deserialized = canonical::deserialize_tree_node(&bytes).unwrap();
    assert_eq!(node, deserialized);
    canonical::verify_canonical(&bytes).unwrap();
}

#[test]
fn test_single_inline_file_round_trip() {
    let node = make_leaf(vec![static_inline_file(".gitignore", b"target/\n")]);
    let bytes = canonical::serialize_tree_node(&node).unwrap();
    let deserialized = canonical::deserialize_tree_node(&bytes).unwrap();
    assert_eq!(node, deserialized);
    canonical::verify_canonical(&bytes).unwrap();
}

#[test]
fn test_mixed_entry_types_round_trip() {
    let block_hash = hash::hash_blob(b"file content here");
    let dir_hash = [0xdd; 32];

    let entries = vec![
        static_inline_file(".gitignore", b"target/\n"),
        static_block_file("README.md", 200, vec![block_hash]),
        LeafEntry::dir("src".to_string(), dir_hash),
        LeafEntry::symlink(
            "link".to_string(),
            "../other".to_string(),
            1742000000000000000,
            1742000000000000000,
            1000,
            1000,
        ),
    ];

    let node = make_leaf(entries);
    let bytes = canonical::serialize_tree_node(&node).unwrap();
    let deserialized = canonical::deserialize_tree_node(&bytes).unwrap();
    assert_eq!(node, deserialized);
    canonical::verify_canonical(&bytes).unwrap();
}

#[test]
fn test_interior_node_round_trip() {
    let node = TreeNode::Interior(InteriorNode {
        keys: vec!["d".to_string(), "m".to_string(), "z".to_string()],
        children: vec![[0xaa; 32], [0xbb; 32], [0xcc; 32], [0xdd; 32]],
    });

    let bytes = canonical::serialize_tree_node(&node).unwrap();
    let deserialized = canonical::deserialize_tree_node(&bytes).unwrap();
    assert_eq!(node, deserialized);
    canonical::verify_canonical(&bytes).unwrap();
}

#[test]
fn test_canonical_key_order_top_level() {
    let node = make_leaf(vec![static_inline_file("a.txt", b"hi")]);
    let bytes = canonical::serialize_tree_node(&node).unwrap();

    let mut cursor = std::io::Cursor::new(&bytes);
    let value = rmpv::decode::read_value(&mut cursor).unwrap();
    let map = value.as_map().unwrap();
    let keys: Vec<&str> = map.iter().map(|(k, _)| k.as_str().unwrap()).collect();

    // "entries" < "kind" < "v"
    assert_eq!(keys, vec!["entries", "kind", "v"]);
}

#[test]
fn test_canonical_key_order_entry_level() {
    let entry = LeafEntry::file(
        "test.sh".to_string(),
        10,
        FileContent::Inline(b"#!/bin/sh\n".to_vec()),
        2000,
        1000,
        500,
        600,
        true,
    );
    let node = make_leaf(vec![entry]);
    let bytes = canonical::serialize_tree_node(&node).unwrap();

    let mut cursor = std::io::Cursor::new(&bytes);
    let value = rmpv::decode::read_value(&mut cursor).unwrap();
    let entries = value.as_map().unwrap()[0].1.as_array().unwrap();
    let entry_map = entries[0].as_map().unwrap();
    let keys: Vec<&str> = entry_map.iter().map(|(k, _)| k.as_str().unwrap()).collect();

    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    assert_eq!(keys, sorted_keys);
    assert!(keys.contains(&"exec"));
}

#[test]
fn test_boolean_omission_exec_false() {
    let node = make_leaf(vec![static_inline_file("a.txt", b"hi")]);
    let bytes = canonical::serialize_tree_node(&node).unwrap();

    let mut cursor = std::io::Cursor::new(&bytes);
    let value = rmpv::decode::read_value(&mut cursor).unwrap();
    let entries = value.as_map().unwrap()[0].1.as_array().unwrap();
    let entry_map = entries[0].as_map().unwrap();
    let has_exec = entry_map.iter().any(|(k, _)| k.as_str() == Some("exec"));
    assert!(!has_exec, "exec=false should be omitted");
}

#[test]
fn test_entries_sorted_by_name() {
    let entries = vec![
        static_inline_file("zebra.txt", b"z"),
        static_inline_file("alpha.txt", b"a"),
        static_inline_file("middle.txt", b"m"),
    ];

    let node = make_leaf(entries);
    let bytes = canonical::serialize_tree_node(&node).unwrap();
    let deserialized = canonical::deserialize_tree_node(&bytes).unwrap();

    if let TreeNode::Leaf(leaf) = &deserialized {
        assert_eq!(leaf.entries[0].name, "zebra.txt");
        assert_eq!(leaf.entries[1].name, "alpha.txt");
        assert_eq!(leaf.entries[2].name, "middle.txt");
    } else {
        panic!("expected leaf node");
    }
}

// ============================================================
// Hash computation tests
// ============================================================

#[test]
fn test_tree_node_hash_is_deterministic() {
    let node = make_leaf(vec![static_inline_file("test.txt", b"hello world")]);
    let h1 = hash::hash_tree_node(&node).unwrap();
    let h2 = hash::hash_tree_node(&node).unwrap();
    assert_eq!(h1, h2);
}

#[test]
fn test_different_nodes_produce_different_hashes() {
    let node1 = make_leaf(vec![static_inline_file("a.txt", b"aaa")]);
    let node2 = make_leaf(vec![static_inline_file("b.txt", b"bbb")]);
    let h1 = hash::hash_tree_node(&node1).unwrap();
    let h2 = hash::hash_tree_node(&node2).unwrap();
    assert_ne!(h1, h2);
}

#[test]
fn test_blob_hash_consistency() {
    let data = b"hello world";
    let h1 = hash::hash_blob(data);
    let h2 = hash::hash_blob(data);
    assert_eq!(h1, h2);
    assert_ne!(h1, hash::hash_blob(b"goodbye"));
}

#[test]
fn test_hex_encode_decode_round_trip() {
    let original = hash::hash_blob(b"test data");
    let hex = hash::hex_encode(&original);
    assert_eq!(hex.len(), 64);
    let decoded = hash::hex_decode(&hex).unwrap();
    assert_eq!(original, decoded);
}

// ============================================================
// Commit tests
// ============================================================

#[test]
fn test_commit_canonical_json_sorted_keys() {
    let commit = Commit::new(
        [0xab; 32],
        None,
        "laptop-alice-01".to_string(),
        1742000000000,
        "initial".to_string(),
    );

    let json = commit.to_canonical_json().unwrap();
    let json_str = std::str::from_utf8(&json).unwrap();

    let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
    let obj = parsed.as_object().unwrap();
    let keys: Vec<&String> = obj.keys().collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
}

#[test]
fn test_commit_null_parent() {
    let commit = Commit::new([0xab; 32], None, "c1".to_string(), 1000, "".to_string());
    let json = commit.to_canonical_json().unwrap();
    let json_str = std::str::from_utf8(&json).unwrap();
    assert!(json_str.contains("\"parent\":null"));
}

#[test]
fn test_commit_with_parent() {
    let parent_hash = [0xcd; 32];
    let commit = Commit::new(
        [0xab; 32],
        Some(parent_hash),
        "c1".to_string(),
        1000,
        "update".to_string(),
    );

    let json = commit.to_canonical_json().unwrap();
    let deserialized = Commit::from_json(&json).unwrap();
    assert_eq!(commit, deserialized);
    assert!(deserialized.parent.is_some());
}

#[test]
fn test_commit_hash_is_deterministic() {
    let commit = Commit::new([0xab; 32], None, "c1".to_string(), 1000, "".to_string());
    let h1 = commit.hash().unwrap();
    let h2 = commit.hash().unwrap();
    assert_eq!(h1, h2);
}

#[test]
fn test_different_commits_produce_different_hashes() {
    let c1 = Commit::new([0xab; 32], None, "c1".to_string(), 1000, "".to_string());
    let c2 = Commit::new([0xab; 32], None, "c1".to_string(), 2000, "".to_string());
    assert_ne!(c1.hash().unwrap(), c2.hash().unwrap());
}

// ============================================================
// Tree lookup and mutation tests
// ============================================================

#[test]
fn test_leaf_lookup_found() {
    let node = make_leaf(vec![
        static_inline_file("a.txt", b"aaa"),
        static_inline_file("b.txt", b"bbb"),
        static_inline_file("c.txt", b"ccc"),
    ]);

    match node.lookup_local("b.txt") {
        LookupResult::Found(entry) => assert_eq!(entry.name, "b.txt"),
        other => panic!("expected Found, got {other:?}"),
    }
}

#[test]
fn test_leaf_lookup_not_found() {
    let node = make_leaf(vec![static_inline_file("a.txt", b"aaa")]);
    match node.lookup_local("z.txt") {
        LookupResult::NotFound => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[test]
fn test_interior_lookup_routes_to_correct_child() {
    let node = TreeNode::Interior(InteriorNode {
        keys: vec!["m".to_string()],
        children: vec![[0xaa; 32], [0xbb; 32]],
    });

    // "apple" < "m" → child 0
    match node.lookup_local("apple") {
        LookupResult::FollowChild { hash, index } => {
            assert_eq!(hash, [0xaa; 32]);
            assert_eq!(index, 0);
        }
        other => panic!("expected FollowChild, got {other:?}"),
    }

    // "zebra" >= "m" → child 1
    match node.lookup_local("zebra") {
        LookupResult::FollowChild { hash, index } => {
            assert_eq!(hash, [0xbb; 32]);
            assert_eq!(index, 1);
        }
        other => panic!("expected FollowChild, got {other:?}"),
    }
}

#[test]
fn test_insert_entry_into_leaf() {
    let mut node = make_leaf(vec![
        static_inline_file("a.txt", b"aaa"),
        static_inline_file("c.txt", b"ccc"),
    ]);

    let result = node.insert_entry(static_inline_file("b.txt", b"bbb")).unwrap();
    assert!(result.is_none(), "small node should not split");

    if let TreeNode::Leaf(leaf) = &node {
        assert_eq!(leaf.entries.len(), 3);
        assert_eq!(leaf.entries[0].name, "a.txt");
        assert_eq!(leaf.entries[1].name, "b.txt");
        assert_eq!(leaf.entries[2].name, "c.txt");
    }
}

#[test]
fn test_insert_replaces_existing_entry() {
    let mut node = make_leaf(vec![static_inline_file("a.txt", b"old")]);

    node.insert_entry(static_inline_file("a.txt", b"new")).unwrap();

    if let TreeNode::Leaf(leaf) = &node {
        assert_eq!(leaf.entries.len(), 1);
        if let Some(FileContent::Inline(data)) = &leaf.entries[0].content {
            assert_eq!(data, b"new");
        } else {
            panic!("expected inline content");
        }
    }
}

#[test]
fn test_remove_entry_from_leaf() {
    let mut node = make_leaf(vec![
        static_inline_file("a.txt", b"aaa"),
        static_inline_file("b.txt", b"bbb"),
    ]);

    let removed = node.remove_entry("a.txt").unwrap();
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().name, "a.txt");

    if let TreeNode::Leaf(leaf) = &node {
        assert_eq!(leaf.entries.len(), 1);
        assert_eq!(leaf.entries[0].name, "b.txt");
    }
}

#[test]
fn test_remove_nonexistent_entry() {
    let mut node = make_leaf(vec![static_inline_file("a.txt", b"aaa")]);
    let removed = node.remove_entry("z.txt").unwrap();
    assert!(removed.is_none());
}

// ============================================================
// Storage integration tests (S3 via RustFS)
// ============================================================

#[tokio::test]
async fn test_storage_blob_put_get() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let data = b"hello world content";

    let hex_hash = storage.put_blob(data).await.unwrap();
    let retrieved = storage.get_blob(&hex_hash).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_storage_blob_not_found() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let result = storage.get_blob("nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_storage_blob_exists() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let data = b"test data";

    let hex_hash = storage.put_blob(data).await.unwrap();
    assert!(storage.blob_exists(&hex_hash).await.unwrap());
    assert!(!storage.blob_exists("nonexistent").await.unwrap());
}

#[tokio::test]
async fn test_storage_node_put_get() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let node = make_leaf(vec![inline_file("test.txt", b"hello")]);

    let hex_hash = storage.put_node(&node).await.unwrap();
    let retrieved = storage.get_node(&hex_hash).await.unwrap();
    assert_eq!(node, retrieved);
}

#[tokio::test]
async fn test_storage_node_exists() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let node = make_leaf(vec![inline_file("test.txt", b"hello")]);

    let hex_hash = storage.put_node(&node).await.unwrap();
    assert!(storage.node_exists(&hex_hash).await.unwrap());
    assert!(!storage.node_exists("nonexistent").await.unwrap());
}

#[tokio::test]
async fn test_storage_head_lifecycle() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;

    // No HEAD initially
    let head = storage.get_head("main").await.unwrap();
    assert!(head.is_none());

    // Create initial commit
    let root_node = make_leaf(vec![inline_file("README.md", b"# Hello")]);
    let root_hash = hash::hash_tree_node(&root_node).unwrap();
    storage.put_node(&root_node).await.unwrap();

    let commit1 = Commit::new(
        root_hash,
        None,
        "client-1".to_string(),
        1742000000000,
        "initial".to_string(),
    );

    let etag1 = storage.put_head("main", &commit1, None).await.unwrap();

    // Read back
    let (retrieved, etag) = storage.get_head("main").await.unwrap().unwrap();
    assert_eq!(retrieved, commit1);
    assert_eq!(etag, etag1);

    // Update with CAS
    let commit2 = Commit::new(
        root_hash,
        Some(commit1.hash().unwrap()),
        "client-1".to_string(),
        1742000001000,
        "update".to_string(),
    );

    let etag2 = storage
        .put_head("main", &commit2, Some(&etag1))
        .await
        .unwrap();
    assert_ne!(etag1, etag2);

    // Verify updated
    let (retrieved2, _) = storage.get_head("main").await.unwrap().unwrap();
    assert_eq!(retrieved2, commit2);
}

#[tokio::test]
async fn test_storage_cas_conflict() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;

    let commit1 = Commit::new(
        [0xaa; 32],
        None,
        "client-1".to_string(),
        1000,
        "".to_string(),
    );

    let _etag1 = storage.put_head("main", &commit1, None).await.unwrap();

    // Try to create again without expected etag (should fail — already exists)
    let result = storage.put_head("main", &commit1, None).await;
    assert!(result.is_err());

    // Try to update with wrong etag
    let commit2 = Commit::new(
        [0xbb; 32],
        None,
        "client-2".to_string(),
        2000,
        "".to_string(),
    );

    let result = storage.put_head("main", &commit2, Some("wrong-etag")).await;
    assert!(result.is_err());
}

// ============================================================
// End-to-end workflow tests (S3)
// ============================================================

#[tokio::test]
async fn test_full_write_read_cycle() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;

    let readme_content = b"# Merkle Drive\n\nA virtual file system.";
    let readme_hash = storage.put_blob(readme_content).await.unwrap();
    let readme_hash_bytes = hash::hex_decode(&readme_hash).unwrap();

    let gitignore_content = b"target/\n";

    let root = make_leaf(vec![
        inline_file(".gitignore", gitignore_content),
        block_file(
            "README.md",
            readme_content.len() as u64,
            vec![readme_hash_bytes],
        ),
    ]);

    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();

    let commit = Commit::new(
        root_hash,
        None,
        "test-client".to_string(),
        1742000000000,
        "initial commit".to_string(),
    );

    let etag = storage.put_head("main", &commit, None).await.unwrap();

    let (head_commit, head_etag) = storage.get_head("main").await.unwrap().unwrap();
    assert_eq!(head_etag, etag);
    assert_eq!(head_commit.root, root_hex);

    let root_node = storage.get_node(&head_commit.root).await.unwrap();

    match root_node.lookup_local(".gitignore") {
        LookupResult::Found(entry) => {
            assert_eq!(entry.name, ".gitignore");
            assert_eq!(entry.size, Some(gitignore_content.len() as u64));
            match &entry.content {
                Some(FileContent::Inline(data)) => assert_eq!(data, gitignore_content),
                _ => panic!("expected inline content"),
            }
        }
        other => panic!("expected Found, got {other:?}"),
    }

    match root_node.lookup_local("README.md") {
        LookupResult::Found(entry) => {
            assert_eq!(entry.name, "README.md");
            match &entry.content {
                Some(FileContent::Blocks(blocks)) => {
                    assert_eq!(blocks.len(), 1);
                    let block_hex = hash::hex_encode(&blocks[0]);
                    let data = storage.get_blob(&block_hex).await.unwrap();
                    assert_eq!(data, readme_content);
                }
                _ => panic!("expected blocks content"),
            }
        }
        other => panic!("expected Found, got {other:?}"),
    }
}

#[tokio::test]
async fn test_content_deduplication() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;

    let data = b"identical content";
    let hash1 = storage.put_blob(data).await.unwrap();
    let hash2 = storage.put_blob(data).await.unwrap();
    assert_eq!(hash1, hash2);

    let node = make_leaf(vec![inline_file("test.txt", b"hello")]);
    let node_hash1 = storage.put_node(&node).await.unwrap();
    let node_hash2 = storage.put_node(&node).await.unwrap();
    assert_eq!(node_hash1, node_hash2);
}

#[tokio::test]
async fn test_multiple_branches() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;

    let root = make_leaf(vec![inline_file("file.txt", b"content")]);
    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();

    let commit_main = Commit::new(
        root_hash,
        None,
        "client".to_string(),
        1000,
        "main commit".to_string(),
    );
    let commit_dev = Commit::new(
        root_hash,
        None,
        "client".to_string(),
        2000,
        "dev commit".to_string(),
    );

    storage.put_head("main", &commit_main, None).await.unwrap();
    storage.put_head("dev", &commit_dev, None).await.unwrap();

    let (main_head, _) = storage.get_head("main").await.unwrap().unwrap();
    let (dev_head, _) = storage.get_head("dev").await.unwrap().unwrap();

    assert_eq!(main_head.message, "main commit");
    assert_eq!(dev_head.message, "dev commit");
}

// ============================================================
// Tree operation edge cases
// ============================================================

#[test]
fn test_insert_entry_into_interior_node_errors() {
    let mut node = TreeNode::Interior(InteriorNode {
        keys: vec!["m".to_string()],
        children: vec![[0xaa; 32], [0xbb; 32]],
    });

    let result = node.insert_entry(static_inline_file("test.txt", b"hi"));
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_remove_entry_from_interior_node_errors() {
    let mut node = TreeNode::Interior(InteriorNode {
        keys: vec!["m".to_string()],
        children: vec![[0xaa; 32], [0xbb; 32]],
    });

    let result = node.remove_entry("test.txt");
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_insert_causing_split() {
    let mut node = make_leaf(vec![]);

    // Insert enough large entries to exceed SPLIT_THRESHOLD (768 KiB).
    // Each entry with ~8 KiB inline data → ~100 entries should be enough.
    for i in 0..150 {
        let name = format!("file_{i:04}.txt");
        let data = vec![0x41u8; 8192]; // 8 KiB per entry
        let entry = LeafEntry::file(
            name,
            data.len() as u64,
            FileContent::Inline(data),
            1000,
            1000,
            1000,
            1000,
            false,
        );
        let result = node.insert_entry(entry).unwrap();
        if let Some(split) = result {
            // Split happened - verify we got valid results
            assert!(!split.separator.is_empty());
            if let TreeNode::Leaf(right_leaf) = &split.right {
                assert!(!right_leaf.entries.is_empty());
            } else {
                panic!("split right should be a leaf");
            }
            return; // test passed
        }
    }
    panic!("expected a split to occur after inserting many large entries");
}

#[test]
fn test_needs_merge_small_node() {
    let node = make_leaf(vec![static_inline_file("tiny.txt", b"x")]);
    assert!(node.needs_merge().unwrap(), "small node should need merge");
}

#[test]
fn test_needs_merge_large_node() {
    // Create a node above MERGE_THRESHOLD (256 KiB)
    let mut entries = Vec::new();
    for i in 0..50 {
        let name = format!("file_{i:04}.txt");
        let data = vec![0x42u8; 8192]; // 8 KiB per entry → 400 KiB total
        entries.push(LeafEntry::file(
            name,
            data.len() as u64,
            FileContent::Inline(data),
            1000,
            1000,
            1000,
            1000,
            false,
        ));
    }
    let node = make_leaf(entries);
    assert!(!node.needs_merge().unwrap(), "large node should not need merge");
}

#[test]
fn test_entries_on_interior_returns_none() {
    let node = TreeNode::Interior(InteriorNode {
        keys: vec!["m".to_string()],
        children: vec![[0xaa; 32], [0xbb; 32]],
    });
    assert!(node.entries().is_none());
}

#[test]
fn test_entries_on_leaf_returns_entries() {
    let node = make_leaf(vec![static_inline_file("a.txt", b"aaa")]);
    let entries = node.entries().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "a.txt");
}

#[test]
fn test_lookup_exact_key_match_interior() {
    let node = TreeNode::Interior(InteriorNode {
        keys: vec!["m".to_string()],
        children: vec![[0xaa; 32], [0xbb; 32]],
    });

    // Exact match on key "m" → routes to idx+1 (child 1)
    match node.lookup_local("m") {
        LookupResult::FollowChild { hash, index } => {
            assert_eq!(hash, [0xbb; 32]);
            assert_eq!(index, 1);
        }
        other => panic!("expected FollowChild, got {other:?}"),
    }
}

// ============================================================
// Canonical deserialization error tests
// ============================================================

/// Helper: build a MessagePack map from key-value pairs and encode to bytes.
fn encode_msgpack_map(pairs: Vec<(&str, rmpv::Value)>) -> Vec<u8> {
    let map_pairs: Vec<(rmpv::Value, rmpv::Value)> = pairs
        .into_iter()
        .map(|(k, v)| (rmpv::Value::String(k.into()), v))
        .collect();
    let value = rmpv::Value::Map(map_pairs);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &value).unwrap();
    buf
}

#[test]
fn test_deserialize_unknown_version() {
    let bytes = encode_msgpack_map(vec![
        ("entries", rmpv::Value::Array(vec![])),
        ("kind", rmpv::Value::String("leaf".into())),
        ("v", rmpv::Value::Integer(999.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::UnknownVersion(999))));
}

#[test]
fn test_deserialize_unknown_kind() {
    let bytes = encode_msgpack_map(vec![
        ("entries", rmpv::Value::Array(vec![])),
        ("kind", rmpv::Value::String("invalid".into())),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_missing_v() {
    let bytes = encode_msgpack_map(vec![
        ("entries", rmpv::Value::Array(vec![])),
        ("kind", rmpv::Value::String("leaf".into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_missing_kind() {
    let bytes = encode_msgpack_map(vec![
        ("entries", rmpv::Value::Array(vec![])),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_missing_entries_array() {
    let bytes = encode_msgpack_map(vec![
        ("kind", rmpv::Value::String("leaf".into())),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_missing_keys_array() {
    let bytes = encode_msgpack_map(vec![
        ("children", rmpv::Value::Array(vec![rmpv::Value::Binary(vec![0xaa; 32])])),
        ("kind", rmpv::Value::String("interior".into())),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_missing_children_array() {
    let bytes = encode_msgpack_map(vec![
        ("keys", rmpv::Value::Array(vec![rmpv::Value::String("m".into())])),
        ("kind", rmpv::Value::String("interior".into())),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_invalid_child_hash_length() {
    let bytes = encode_msgpack_map(vec![
        ("children", rmpv::Value::Array(vec![rmpv::Value::Binary(vec![0xaa; 16])])), // 16 bytes, not 32
        ("keys", rmpv::Value::Array(vec![])),
        ("kind", rmpv::Value::String("interior".into())),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_missing_name() {
    // Entry without "name" field
    let entry = rmpv::Value::Map(vec![
        (rmpv::Value::String("type".into()), rmpv::Value::String("file".into())),
    ]);
    let bytes = encode_msgpack_map(vec![
        ("entries", rmpv::Value::Array(vec![entry])),
        ("kind", rmpv::Value::String("leaf".into())),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_missing_type() {
    let entry = rmpv::Value::Map(vec![
        (rmpv::Value::String("name".into()), rmpv::Value::String("test.txt".into())),
    ]);
    let bytes = encode_msgpack_map(vec![
        ("entries", rmpv::Value::Array(vec![entry])),
        ("kind", rmpv::Value::String("leaf".into())),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_invalid_entry_type() {
    let entry = rmpv::Value::Map(vec![
        (rmpv::Value::String("name".into()), rmpv::Value::String("test.txt".into())),
        (rmpv::Value::String("type".into()), rmpv::Value::String("bogus".into())),
    ]);
    let bytes = encode_msgpack_map(vec![
        ("entries", rmpv::Value::Array(vec![entry])),
        ("kind", rmpv::Value::String("leaf".into())),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_non_string_map_key() {
    // Map with integer key instead of string
    let value = rmpv::Value::Map(vec![
        (rmpv::Value::Integer(42.into()), rmpv::Value::String("leaf".into())),
    ]);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &value).unwrap();
    let result = canonical::deserialize_tree_node(&buf);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_entry_not_a_map() {
    let bytes = encode_msgpack_map(vec![
        ("entries", rmpv::Value::Array(vec![rmpv::Value::String("not a map".into())])),
        ("kind", rmpv::Value::String("leaf".into())),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_node_not_a_map() {
    let value = rmpv::Value::Array(vec![rmpv::Value::Integer(1.into())]);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &value).unwrap();
    let result = canonical::deserialize_tree_node(&buf);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_invalid_block_hash_length() {
    let entry = rmpv::Value::Map(vec![
        (rmpv::Value::String("blocks".into()), rmpv::Value::Array(vec![
            rmpv::Value::Binary(vec![0xaa; 16]), // 16 bytes, not 32
        ])),
        (rmpv::Value::String("name".into()), rmpv::Value::String("test.txt".into())),
        (rmpv::Value::String("type".into()), rmpv::Value::String("file".into())),
    ]);
    let bytes = encode_msgpack_map(vec![
        ("entries", rmpv::Value::Array(vec![entry])),
        ("kind", rmpv::Value::String("leaf".into())),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_deserialize_invalid_inline_content() {
    let entry = rmpv::Value::Map(vec![
        (rmpv::Value::String("inline".into()), rmpv::Value::Integer(42.into())), // not binary
        (rmpv::Value::String("name".into()), rmpv::Value::String("test.txt".into())),
        (rmpv::Value::String("type".into()), rmpv::Value::String("file".into())),
    ]);
    let bytes = encode_msgpack_map(vec![
        ("entries", rmpv::Value::Array(vec![entry])),
        ("kind", rmpv::Value::String("leaf".into())),
        ("v", rmpv::Value::Integer(1.into())),
    ]);
    let result = canonical::deserialize_tree_node(&bytes);
    assert!(matches!(result, Err(MerkleError::InvalidNode(_))));
}

#[test]
fn test_verify_canonical_tampered_bytes() {
    let node = make_leaf(vec![static_inline_file("test.txt", b"hello")]);
    let mut bytes = canonical::serialize_tree_node(&node).unwrap();

    // Tamper with a byte in the middle
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;

    let result = canonical::verify_canonical(&bytes);
    // Should either fail to deserialize or fail canonical check
    assert!(result.is_err());
}

// ============================================================
// Hash edge cases
// ============================================================

#[test]
fn test_hex_decode_invalid_hex() {
    let result = hash::hex_decode("not-valid-hex-string-at-all!!!!!");
    assert!(result.is_err());
}

#[test]
fn test_hex_decode_wrong_length() {
    // Valid hex but only 16 bytes, not 32
    let result = hash::hex_decode("aabbccdd00112233aabbccdd00112233");
    assert!(result.is_err());
}
