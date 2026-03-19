//! Integration tests that mount a merkle-drive FUSE filesystem backed by a real
//! RustFS (S3-compatible) endpoint running in Docker via testcontainers.
//!
//! These tests exercise the FUSE interface: writing and reading files through
//! standard filesystem operations, then verifying that data persists correctly
//! in the S3-backed storage.

mod common;

use std::os::unix::fs::symlink as unix_symlink;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use merkle_drive::commit::Commit;
use merkle_drive::hash;
use merkle_drive::tree::*;

use common::*;

// ============================================================
// Existing integration tests (refactored to use common::*)
// ============================================================

#[tokio::test]
async fn test_fuse_write_and_read_file() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
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
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;
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
    let data = read_file(&storage2, "main", "persisted.txt").await;
    assert_eq!(data, b"this should persist");
}

#[tokio::test]
async fn test_fuse_read_preexisting_data() {
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;

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
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;
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

    let a = read_file(&storage2, "main", "alpha.txt").await;
    let b = read_file(&storage2, "main", "beta.txt").await;
    let g = read_file(&storage2, "main", "gamma.txt").await;
    assert_eq!(a, b"aaa");
    assert_eq!(b, b"bbb");
    assert_eq!(g, b"ggg");
}

#[tokio::test]
async fn test_fuse_nested_directories() {
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;
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

    let main_rs = read_nested_file(&storage2, "main", "src", "main.rs").await;
    let readme = read_nested_file(&storage2, "main", "docs", "readme.md").await;
    let cargo = read_file(&storage2, "main", "Cargo.toml").await;

    assert_eq!(main_rs, b"fn main() {}");
    assert_eq!(readme, b"# Docs");
    assert_eq!(cargo, b"[package]");
}

#[tokio::test]
async fn test_fuse_overwrite_file() {
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;
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

    let data = read_file(&storage2, "main", "data.txt").await;
    assert_eq!(data, b"version 2");
}

#[tokio::test]
async fn test_fuse_delete_file() {
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;
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
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
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
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;
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

    let data = read_file(&storage2, "main", "large.bin").await;
    assert_eq!(data, expected);
}

#[tokio::test]
async fn test_fuse_rmdir() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
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
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;

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

// ============================================================
// New error path tests
// ============================================================

#[tokio::test]
async fn test_fuse_rmdir_nonempty() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    let err = blocking(move || {
        std::fs::create_dir(mp2.join("mydir")).unwrap();
        std::fs::write(mp2.join("mydir").join("child.txt"), b"content").unwrap();
        // Try to rmdir without removing the file first
        std::fs::remove_dir(mp2.join("mydir")).unwrap_err()
    })
    .await;

    assert_eq!(
        err.raw_os_error(),
        Some(libc::ENOTEMPTY),
        "rmdir on non-empty dir should return ENOTEMPTY, got: {err}"
    );

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_unlink_nonexistent() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    let err = blocking(move || std::fs::remove_file(mp2.join("ghost.txt")).unwrap_err()).await;

    assert_eq!(
        err.raw_os_error(),
        Some(libc::ENOENT),
        "unlink on nonexistent file should return ENOENT, got: {err}"
    );

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_rename_same_dir() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("old_name.txt"), b"rename me").unwrap();
        std::fs::rename(mp2.join("old_name.txt"), mp2.join("new_name.txt")).unwrap();
    })
    .await;

    // Old name should be gone
    let mp2 = mp.clone();
    let old_exists = blocking(move || mp2.join("old_name.txt").exists()).await;
    assert!(!old_exists, "old_name.txt should not exist after rename");

    // New name should have the content
    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("new_name.txt")).unwrap()).await;
    assert_eq!(content, b"rename me");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_rename_across_dirs() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("dir_a")).unwrap();
        std::fs::create_dir(mp2.join("dir_b")).unwrap();
        std::fs::write(mp2.join("dir_a").join("moved.txt"), b"cross-dir data").unwrap();
        std::fs::rename(
            mp2.join("dir_a").join("moved.txt"),
            mp2.join("dir_b").join("moved.txt"),
        )
        .unwrap();
    })
    .await;

    // Should be gone from dir_a
    let mp2 = mp.clone();
    let old_exists = blocking(move || mp2.join("dir_a").join("moved.txt").exists()).await;
    assert!(!old_exists, "file should not exist in dir_a after rename");

    // Should be present in dir_b
    let mp2 = mp.clone();
    let content =
        blocking(move || std::fs::read(mp2.join("dir_b").join("moved.txt")).unwrap()).await;
    assert_eq!(content, b"cross-dir data");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_rename_overwrite() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("src.txt"), b"new content").unwrap();
        std::fs::write(mp2.join("dst.txt"), b"old content").unwrap();
        // Rename src onto dst, should replace dst
        std::fs::rename(mp2.join("src.txt"), mp2.join("dst.txt")).unwrap();
    })
    .await;

    // src should be gone
    let mp2 = mp.clone();
    let src_exists = blocking(move || mp2.join("src.txt").exists()).await;
    assert!(!src_exists, "src.txt should not exist after rename");

    // dst should have the new content
    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("dst.txt")).unwrap()).await;
    assert_eq!(content, b"new content");

    mount.unmount();
}

// ============================================================
// New happy path / boundary tests
// ============================================================

#[tokio::test]
async fn test_fuse_empty_file() {
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Write a 0-byte file
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("empty.txt"), b"").unwrap();
    })
    .await;

    // Read it back via FUSE
    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("empty.txt")).unwrap()).await;
    assert_eq!(content, b"");

    // Check metadata shows size 0
    let mp2 = mp.clone();
    let size = blocking(move || std::fs::metadata(mp2.join("empty.txt")).unwrap().len()).await;
    assert_eq!(size, 0);

    // Unmount and verify persistence
    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let data = read_file(&storage2, "main", "empty.txt").await;
    assert_eq!(data, b"");
}

#[tokio::test]
async fn test_fuse_file_at_inline_threshold() {
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Exactly 128 bytes - should be stored inline on commit
    let data_128: Vec<u8> = (0..128).map(|i| (i % 256) as u8).collect();
    let expected = data_128.clone();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("inline.bin"), &data_128).unwrap();
    })
    .await;

    // Read back via FUSE
    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("inline.bin")).unwrap()).await;
    assert_eq!(content, expected);

    // Unmount and verify persistence
    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let data = read_file(&storage2, "main", "inline.bin").await;
    assert_eq!(data.len(), 128);
    assert_eq!(data, expected);
}

#[tokio::test]
async fn test_fuse_file_above_inline_threshold() {
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // 129 bytes - should use blocks on commit
    let data_129: Vec<u8> = (0..129).map(|i| (i % 256) as u8).collect();
    let expected = data_129.clone();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("blocks.bin"), &data_129).unwrap();
    })
    .await;

    // Read back via FUSE
    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("blocks.bin")).unwrap()).await;
    assert_eq!(content, expected);

    // Unmount and verify persistence
    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let data = read_file(&storage2, "main", "blocks.bin").await;
    assert_eq!(data.len(), 129);
    assert_eq!(data, expected);
}

#[tokio::test]
async fn test_fuse_deeply_nested_dirs() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Create 10 levels deep: a/b/c/d/e/f/g/h/i/j/file.txt
    let mp2 = mp.clone();
    blocking(move || {
        let mut path = mp2.clone();
        for dir in &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"] {
            path = path.join(dir);
            std::fs::create_dir(&path).unwrap();
        }
        std::fs::write(path.join("file.txt"), b"deep content").unwrap();
    })
    .await;

    // Read back via FUSE
    let mp2 = mp.clone();
    let content = blocking(move || {
        std::fs::read(
            mp2.join("a")
                .join("b")
                .join("c")
                .join("d")
                .join("e")
                .join("f")
                .join("g")
                .join("h")
                .join("i")
                .join("j")
                .join("file.txt"),
        )
        .unwrap()
    })
    .await;
    assert_eq!(content, b"deep content");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_executable_permission() {
    let container = RustfsContainer::start().await;
    let (storage, storage2) = container.new_storage_pair().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("script.sh"), b"#!/bin/sh\necho hello").unwrap();
        let mut perms = std::fs::metadata(mp2.join("script.sh")).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(mp2.join("script.sh"), perms).unwrap();
    })
    .await;

    // Verify permission via FUSE
    let mp2 = mp.clone();
    let mode = blocking(move || {
        std::fs::metadata(mp2.join("script.sh"))
            .unwrap()
            .permissions()
            .mode()
    })
    .await;
    // Check executable bit is set (at least user execute)
    assert!(
        mode & 0o100 != 0,
        "executable bit should be set, got mode: {mode:#o}"
    );

    // Unmount and verify persistence: the exec flag should survive commit
    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (commit, _) = storage2.get_head("main").await.unwrap().unwrap();
    let root_node = storage2.get_node(&commit.root).await.unwrap();
    match root_node.lookup_local("script.sh") {
        LookupResult::Found(entry) => {
            assert!(entry.exec, "script.sh should be marked executable after commit");
        }
        other => panic!("expected Found for script.sh, got {other:?}"),
    }
}

#[tokio::test]
async fn test_fuse_empty_dir_readdir() {
    let container = RustfsContainer::start().await;
    let storage = container.new_storage().await;
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("emptydir")).unwrap();
    })
    .await;

    // readdir on empty dir should return only "." and ".."
    let mp2 = mp.clone();
    let entries: Vec<String> = blocking(move || {
        std::fs::read_dir(mp2.join("emptydir"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_str().unwrap().to_string())
            .collect()
    })
    .await;

    // std::fs::read_dir does not return "." and ".." on Linux, so the vec should be empty
    assert!(
        entries.is_empty(),
        "empty dir should have no entries from read_dir, got: {entries:?}"
    );

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_persistence_remount() {
    let container = RustfsContainer::start().await;
    let (storage1, storage2) = container.new_storage_pair().await;

    // First mount: write files
    let mut mount1 = FuseMount::new(storage1, "main").await;
    let mp1 = mount1.path();

    let mp = mp1.clone();
    blocking(move || {
        std::fs::create_dir(mp.join("projects")).unwrap();
        std::fs::write(mp.join("projects").join("code.rs"), b"fn main() {}").unwrap();
        std::fs::write(mp.join("notes.txt"), b"important notes").unwrap();
        std::fs::write(mp.join("config.toml"), b"[settings]\nkey = \"value\"").unwrap();
    })
    .await;

    // Unmount (triggers commit)
    mount1.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Second mount with a different client pointing at same buckets
    let mut mount2 = FuseMount::new(storage2, "main").await;
    let mp2 = mount2.path();

    // Verify all files are present and correct
    let mp = mp2.clone();
    let results = blocking(move || {
        (
            std::fs::read(mp.join("projects").join("code.rs")).unwrap(),
            std::fs::read(mp.join("notes.txt")).unwrap(),
            std::fs::read(mp.join("config.toml")).unwrap(),
        )
    })
    .await;

    assert_eq!(results.0, b"fn main() {}");
    assert_eq!(results.1, b"important notes");
    assert_eq!(results.2, b"[settings]\nkey = \"value\"");

    // Verify directory listing works for nested dir
    let mp = mp2.clone();
    let mut names: Vec<String> = blocking(move || {
        std::fs::read_dir(mp.join("projects"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_str().unwrap().to_string())
            .collect()
    })
    .await;
    names.sort();
    assert_eq!(names, vec!["code.rs"]);

    // Verify root directory listing
    let mp = mp2.clone();
    let mut root_names: Vec<String> = blocking(move || {
        std::fs::read_dir(&mp)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_str().unwrap().to_string())
            .collect()
    })
    .await;
    root_names.sort();
    assert_eq!(root_names, vec!["config.toml", "notes.txt", "projects"]);

    mount2.unmount();
}
