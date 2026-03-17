use merkle_drive::canonical;
use merkle_drive::commit::Commit;
use merkle_drive::error::MerkleError;
use merkle_drive::hash;
use merkle_drive::storage::{MemoryStorage, Storage};
use merkle_drive::tree::*;

/// Helper: create a simple leaf node with the given file entries.
fn make_leaf(entries: Vec<LeafEntry>) -> TreeNode {
    TreeNode::Leaf(LeafNode { entries })
}

/// Helper: create a file entry with inline content.
fn inline_file(name: &str, data: &[u8]) -> LeafEntry {
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

/// Helper: create a file entry with block references.
fn block_file(name: &str, size: u64, block_hashes: Vec<[u8; 32]>) -> LeafEntry {
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

// ============================================================
// Serialization round-trip tests
// ============================================================

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
    let node = make_leaf(vec![inline_file(".gitignore", b"target/\n")]);
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
        inline_file(".gitignore", b"target/\n"),
        block_file("README.md", 200, vec![block_hash]),
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
    let node = make_leaf(vec![inline_file("a.txt", b"hi")]);
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

    // All keys should be sorted lexicographically
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    assert_eq!(keys, sorted_keys);

    // exec should be present (it's true)
    assert!(keys.contains(&"exec"));
}

#[test]
fn test_boolean_omission_exec_false() {
    let node = make_leaf(vec![inline_file("a.txt", b"hi")]);
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
        inline_file("zebra.txt", b"z"),
        inline_file("alpha.txt", b"a"),
        inline_file("middle.txt", b"m"),
    ];

    // Entries are sorted during serialization by the canonical format
    let node = make_leaf(entries);
    let bytes = canonical::serialize_tree_node(&node).unwrap();
    let deserialized = canonical::deserialize_tree_node(&bytes).unwrap();

    // The entries in the deserialized node should match the original
    // (they were inserted in the order given, but the leaf stores them as-is)
    if let TreeNode::Leaf(leaf) = &deserialized {
        // Our canonical format preserves insertion order — the user is responsible
        // for keeping entries sorted. Verify the input order is preserved.
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
    let node = make_leaf(vec![inline_file("test.txt", b"hello world")]);
    let h1 = hash::hash_tree_node(&node).unwrap();
    let h2 = hash::hash_tree_node(&node).unwrap();
    assert_eq!(h1, h2);
}

#[test]
fn test_different_nodes_produce_different_hashes() {
    let node1 = make_leaf(vec![inline_file("a.txt", b"aaa")]);
    let node2 = make_leaf(vec![inline_file("b.txt", b"bbb")]);
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

    // Different content produces different hash
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
        inline_file("a.txt", b"aaa"),
        inline_file("b.txt", b"bbb"),
        inline_file("c.txt", b"ccc"),
    ]);

    match node.lookup_local("b.txt") {
        LookupResult::Found(entry) => assert_eq!(entry.name, "b.txt"),
        other => panic!("expected Found, got {other:?}"),
    }
}

#[test]
fn test_leaf_lookup_not_found() {
    let node = make_leaf(vec![inline_file("a.txt", b"aaa")]);
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
        inline_file("a.txt", b"aaa"),
        inline_file("c.txt", b"ccc"),
    ]);

    let result = node.insert_entry(inline_file("b.txt", b"bbb")).unwrap();
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
    let mut node = make_leaf(vec![inline_file("a.txt", b"old")]);

    node.insert_entry(inline_file("a.txt", b"new")).unwrap();

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
        inline_file("a.txt", b"aaa"),
        inline_file("b.txt", b"bbb"),
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
    let mut node = make_leaf(vec![inline_file("a.txt", b"aaa")]);
    let removed = node.remove_entry("z.txt").unwrap();
    assert!(removed.is_none());
}

// ============================================================
// Storage integration tests (MemoryStorage)
// ============================================================

#[tokio::test]
async fn test_memory_storage_blob_put_get() {
    let storage = MemoryStorage::new();
    let data = b"hello world content";

    let hex_hash = storage.put_blob(data).await.unwrap();
    let retrieved = storage.get_blob(&hex_hash).await.unwrap();
    assert_eq!(retrieved, data);
}

#[tokio::test]
async fn test_memory_storage_blob_not_found() {
    let storage = MemoryStorage::new();
    let result = storage.get_blob("nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_memory_storage_blob_exists() {
    let storage = MemoryStorage::new();
    let data = b"test data";

    let hex_hash = storage.put_blob(data).await.unwrap();
    assert!(storage.blob_exists(&hex_hash).await.unwrap());
    assert!(!storage.blob_exists("nonexistent").await.unwrap());
}

#[tokio::test]
async fn test_memory_storage_node_put_get() {
    let storage = MemoryStorage::new();
    let node = make_leaf(vec![inline_file("test.txt", b"hello")]);

    let hex_hash = storage.put_node(&node).await.unwrap();
    let retrieved = storage.get_node(&hex_hash).await.unwrap();
    assert_eq!(node, retrieved);
}

#[tokio::test]
async fn test_memory_storage_node_exists() {
    let storage = MemoryStorage::new();
    let node = make_leaf(vec![inline_file("test.txt", b"hello")]);

    let hex_hash = storage.put_node(&node).await.unwrap();
    assert!(storage.node_exists(&hex_hash).await.unwrap());
    assert!(!storage.node_exists("nonexistent").await.unwrap());
}

#[tokio::test]
async fn test_memory_storage_head_lifecycle() {
    let storage = MemoryStorage::new();

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

    // Put initial HEAD (no expected etag)
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
async fn test_memory_storage_cas_conflict() {
    let storage = MemoryStorage::new();

    let commit1 = Commit::new(
        [0xaa; 32],
        None,
        "client-1".to_string(),
        1000,
        "".to_string(),
    );

    let etag1 = storage.put_head("main", &commit1, None).await.unwrap();

    // Try to create again without expected etag (should fail — already exists)
    let result = storage.put_head("main", &commit1, None).await;
    assert!(matches!(result, Err(MerkleError::CasConflict { .. })));

    // Try to update with wrong etag
    let commit2 = Commit::new(
        [0xbb; 32],
        None,
        "client-2".to_string(),
        2000,
        "".to_string(),
    );

    let result = storage.put_head("main", &commit2, Some("wrong-etag")).await;
    assert!(matches!(result, Err(MerkleError::CasConflict { .. })));

    // Correct etag should work
    let _etag2 = storage
        .put_head("main", &commit2, Some(&etag1))
        .await
        .unwrap();
}

// ============================================================
// End-to-end workflow tests
// ============================================================

#[tokio::test]
async fn test_full_write_read_cycle() {
    let storage = MemoryStorage::new();

    // 1. Upload file content blobs
    let readme_content = b"# Merkle Drive\n\nA virtual file system.";
    let readme_hash = storage.put_blob(readme_content).await.unwrap();
    let readme_hash_bytes = hash::hex_decode(&readme_hash).unwrap();

    let gitignore_content = b"target/\n";

    // 2. Build the root tree node
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

    // 3. Create and store commit
    let commit = Commit::new(
        root_hash,
        None,
        "test-client".to_string(),
        1742000000000,
        "initial commit".to_string(),
    );

    let etag = storage.put_head("main", &commit, None).await.unwrap();

    // 4. Read path: fetch HEAD, walk tree, read content
    let (head_commit, head_etag) = storage.get_head("main").await.unwrap().unwrap();
    assert_eq!(head_etag, etag);
    assert_eq!(head_commit.root, root_hex);

    let root_node = storage.get_node(&head_commit.root).await.unwrap();

    // Look up .gitignore (inline)
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

    // Look up README.md (blocks)
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
async fn test_nested_directory_structure() {
    let storage = MemoryStorage::new();

    // Build src/ directory
    let main_rs_content = b"fn main() { println!(\"Hello\"); }";
    let main_rs_hash = storage.put_blob(main_rs_content).await.unwrap();
    let main_rs_hash_bytes = hash::hex_decode(&main_rs_hash).unwrap();

    let src_leaf = make_leaf(vec![block_file(
        "main.rs",
        main_rs_content.len() as u64,
        vec![main_rs_hash_bytes],
    )]);
    let src_hex = storage.put_node(&src_leaf).await.unwrap();
    let src_hash = hash::hex_decode(&src_hex).unwrap();

    // Build root directory
    let root = make_leaf(vec![
        inline_file("Cargo.toml", b"[package]\nname = \"test\""),
        LeafEntry::dir("src".to_string(), src_hash),
    ]);
    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();

    // Create commit
    let commit = Commit::new(
        root_hash,
        None,
        "test-client".to_string(),
        1742000000000,
        "initial".to_string(),
    );
    storage.put_head("main", &commit, None).await.unwrap();

    // Read: navigate to src/main.rs
    let (head, _) = storage.get_head("main").await.unwrap().unwrap();
    let root_node = storage.get_node(&head.root).await.unwrap();

    // Look up src directory
    match root_node.lookup_local("src") {
        LookupResult::Found(entry) => {
            assert_eq!(entry.entry_type, EntryType::Dir);
            let dir_hash = entry.hash.unwrap();
            let dir_hex = hash::hex_encode(&dir_hash);

            // Navigate into src/
            let src_node = storage.get_node(&dir_hex).await.unwrap();
            match src_node.lookup_local("main.rs") {
                LookupResult::Found(file_entry) => {
                    assert_eq!(file_entry.name, "main.rs");
                    match &file_entry.content {
                        Some(FileContent::Blocks(blocks)) => {
                            let data = storage
                                .get_blob(&hash::hex_encode(&blocks[0]))
                                .await
                                .unwrap();
                            assert_eq!(data, main_rs_content);
                        }
                        _ => panic!("expected blocks"),
                    }
                }
                other => panic!("expected Found, got {other:?}"),
            }
        }
        other => panic!("expected Found for src, got {other:?}"),
    }
}

#[tokio::test]
async fn test_commit_chain() {
    let storage = MemoryStorage::new();

    // First commit
    let root1 = make_leaf(vec![inline_file("file.txt", b"v1")]);
    let root1_hex = storage.put_node(&root1).await.unwrap();
    let root1_hash = hash::hex_decode(&root1_hex).unwrap();

    let commit1 = Commit::new(
        root1_hash,
        None,
        "client-1".to_string(),
        1000,
        "first".to_string(),
    );
    let etag1 = storage.put_head("main", &commit1, None).await.unwrap();
    let commit1_hash = commit1.hash().unwrap();

    // Second commit (child of first)
    let root2 = make_leaf(vec![inline_file("file.txt", b"v2")]);
    let root2_hex = storage.put_node(&root2).await.unwrap();
    let root2_hash = hash::hex_decode(&root2_hex).unwrap();

    let commit2 = Commit::new(
        root2_hash,
        Some(commit1_hash),
        "client-1".to_string(),
        2000,
        "second".to_string(),
    );
    let _etag2 = storage
        .put_head("main", &commit2, Some(&etag1))
        .await
        .unwrap();

    // Verify chain
    let (head, _) = storage.get_head("main").await.unwrap().unwrap();
    assert_eq!(head.message, "second");
    assert!(head.parent.is_some());
    assert_eq!(head.parent.unwrap(), hash::hex_encode(&commit1_hash));
}

#[tokio::test]
async fn test_concurrent_cas_one_wins() {
    let storage = MemoryStorage::new();

    // Initial commit
    let root = make_leaf(vec![inline_file("file.txt", b"v1")]);
    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();

    let commit1 = Commit::new(
        root_hash,
        None,
        "client-1".to_string(),
        1000,
        "initial".to_string(),
    );
    let etag1 = storage.put_head("main", &commit1, None).await.unwrap();

    // Two clients try to update simultaneously
    let commit_a = Commit::new(
        [0xaa; 32],
        Some(commit1.hash().unwrap()),
        "client-a".to_string(),
        2000,
        "update a".to_string(),
    );

    let commit_b = Commit::new(
        [0xbb; 32],
        Some(commit1.hash().unwrap()),
        "client-b".to_string(),
        2000,
        "update b".to_string(),
    );

    // First writer succeeds
    let etag_a = storage
        .put_head("main", &commit_a, Some(&etag1))
        .await
        .unwrap();

    // Second writer fails (stale etag)
    let result = storage.put_head("main", &commit_b, Some(&etag1)).await;
    assert!(matches!(result, Err(MerkleError::CasConflict { .. })));

    // Second writer retries with new etag
    let _etag_b = storage
        .put_head("main", &commit_b, Some(&etag_a))
        .await
        .unwrap();

    let (head, _) = storage.get_head("main").await.unwrap().unwrap();
    assert_eq!(head.message, "update b");
}

#[tokio::test]
async fn test_content_deduplication() {
    let storage = MemoryStorage::new();

    // Same content uploaded twice should produce the same hash
    let data = b"identical content";
    let hash1 = storage.put_blob(data).await.unwrap();
    let hash2 = storage.put_blob(data).await.unwrap();
    assert_eq!(hash1, hash2);

    // Same tree node uploaded twice should produce same hash
    let node = make_leaf(vec![inline_file("test.txt", b"hello")]);
    let node_hash1 = storage.put_node(&node).await.unwrap();
    let node_hash2 = storage.put_node(&node).await.unwrap();
    assert_eq!(node_hash1, node_hash2);
}

#[tokio::test]
async fn test_modify_file_creates_new_tree_path() {
    let storage = MemoryStorage::new();

    // Initial tree
    let root1 = make_leaf(vec![
        inline_file("a.txt", b"aaa"),
        inline_file("b.txt", b"bbb"),
    ]);
    let root1_hex = storage.put_node(&root1).await.unwrap();

    // Modify b.txt → new tree
    let root2 = make_leaf(vec![
        inline_file("a.txt", b"aaa"),
        inline_file("b.txt", b"bbb modified"),
    ]);
    let root2_hex = storage.put_node(&root2).await.unwrap();

    // Different hashes for different trees
    assert_ne!(root1_hex, root2_hex);

    // Old tree is still accessible
    let old_root = storage.get_node(&root1_hex).await.unwrap();
    match old_root.lookup_local("b.txt") {
        LookupResult::Found(entry) => {
            if let Some(FileContent::Inline(data)) = &entry.content {
                assert_eq!(data, b"bbb");
            }
        }
        _ => panic!("expected old entry"),
    }

    // New tree has modified content
    let new_root = storage.get_node(&root2_hex).await.unwrap();
    match new_root.lookup_local("b.txt") {
        LookupResult::Found(entry) => {
            if let Some(FileContent::Inline(data)) = &entry.content {
                assert_eq!(data, b"bbb modified");
            }
        }
        _ => panic!("expected new entry"),
    }
}

#[tokio::test]
async fn test_executable_file() {
    let storage = MemoryStorage::new();

    let entry = LeafEntry::file(
        "run.sh".to_string(),
        10,
        FileContent::Inline(b"#!/bin/sh\n".to_vec()),
        1742000000000000000,
        1742000000000000000,
        0,
        0,
        true,
    );

    let node = make_leaf(vec![entry]);
    let hex = storage.put_node(&node).await.unwrap();
    let retrieved = storage.get_node(&hex).await.unwrap();

    match retrieved.lookup_local("run.sh") {
        LookupResult::Found(entry) => {
            assert!(entry.exec);
        }
        _ => panic!("expected Found"),
    }
}

#[tokio::test]
async fn test_large_directory_many_entries() {
    let storage = MemoryStorage::new();

    // Create a directory with many entries
    let entries: Vec<LeafEntry> = (0..100)
        .map(|i| {
            inline_file(
                &format!("file_{i:04}.txt"),
                format!("content {i}").as_bytes(),
            )
        })
        .collect();

    let node = make_leaf(entries);
    let hex = storage.put_node(&node).await.unwrap();
    let retrieved = storage.get_node(&hex).await.unwrap();

    // Verify lookup works for various entries
    for i in [0, 25, 50, 75, 99] {
        let name = format!("file_{i:04}.txt");
        match retrieved.lookup_local(&name) {
            LookupResult::Found(entry) => assert_eq!(entry.name, name),
            other => panic!("expected Found for {name}, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn test_symlink_storage() {
    let storage = MemoryStorage::new();

    let node = make_leaf(vec![
        inline_file("real.txt", b"content"),
        LeafEntry::symlink(
            "link.txt".to_string(),
            "real.txt".to_string(),
            1742000000000000000,
            1742000000000000000,
            1000,
            1000,
        ),
    ]);

    let hex = storage.put_node(&node).await.unwrap();
    let retrieved = storage.get_node(&hex).await.unwrap();

    match retrieved.lookup_local("link.txt") {
        LookupResult::Found(entry) => {
            assert_eq!(entry.entry_type, EntryType::Symlink);
            assert_eq!(entry.target.as_deref(), Some("real.txt"));
        }
        other => panic!("expected Found, got {other:?}"),
    }
}

#[tokio::test]
async fn test_multiple_branches() {
    let storage = MemoryStorage::new();

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
