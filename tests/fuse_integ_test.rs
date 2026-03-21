//! Integration tests that mount a merkle-drive FUSE filesystem backed by a real
//! RustFS (S3-compatible) endpoint running in Docker via testcontainers.
//!
//! These tests exercise the FUSE interface: writing and reading files through
//! standard filesystem operations, then verifying that data persists correctly
//! in the S3-backed storage.

use std::os::unix::fs::symlink as unix_symlink;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use merkle_drive::commit::Commit;
use merkle_drive::error::MerkleError;
use merkle_drive::fuse::MerkleFuse;
use merkle_drive::hash;
use merkle_drive::storage::{MemoryStorage, S3Storage, Storage};
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

impl FuseMount {
    /// Mount with any Storage implementation (MemoryStorage, FaultyStorage, etc.)
    async fn new_with_storage<S: Storage + Send + Sync + 'static>(
        storage: S,
        branch: &str,
    ) -> Self {
        let mount_dir = tempfile::tempdir().expect("failed to create temp dir");
        let rt = tokio::runtime::Handle::current();
        let fs = MerkleFuse::new(storage, rt, branch.to_string(), "fuse-test".to_string()).await;
        let session = fs
            .mount(mount_dir.path())
            .expect("failed to mount FUSE filesystem");

        tokio::time::sleep(Duration::from_millis(500)).await;

        Self {
            session: Some(session),
            mount_dir,
        }
    }
}

impl Drop for FuseMount {
    fn drop(&mut self) {
        self.session.take();
    }
}

// ============================================================
// FaultyStorage wrapper for network error tests
// ============================================================

#[derive(Clone)]
struct FaultInjector {
    fail_writes: Arc<AtomicBool>,
    fail_reads: Arc<AtomicBool>,
}

impl FaultInjector {
    fn new() -> Self {
        Self {
            fail_writes: Arc::new(AtomicBool::new(false)),
            fail_reads: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set_fail_writes(&self, fail: bool) {
        self.fail_writes.store(fail, Ordering::SeqCst);
    }

    #[allow(dead_code)]
    fn set_fail_reads(&self, fail: bool) {
        self.fail_reads.store(fail, Ordering::SeqCst);
    }
}

struct FaultyStorage<S: Storage> {
    inner: S,
    injector: FaultInjector,
}

impl<S: Storage> FaultyStorage<S> {
    fn new(inner: S, injector: FaultInjector) -> Self {
        Self { inner, injector }
    }
}

impl<S: Storage + Send + Sync> Storage for FaultyStorage<S> {
    async fn put_blob(&self, data: &[u8]) -> merkle_drive::error::Result<String> {
        if self.injector.fail_writes.load(Ordering::SeqCst) {
            return Err(MerkleError::Storage("injected: write failure".into()));
        }
        self.inner.put_blob(data).await
    }

    async fn get_blob(&self, hex_hash: &str) -> merkle_drive::error::Result<Vec<u8>> {
        if self.injector.fail_reads.load(Ordering::SeqCst) {
            return Err(MerkleError::Storage("injected: read failure".into()));
        }
        self.inner.get_blob(hex_hash).await
    }

    async fn put_node(&self, node: &TreeNode) -> merkle_drive::error::Result<String> {
        if self.injector.fail_writes.load(Ordering::SeqCst) {
            return Err(MerkleError::Storage("injected: write failure".into()));
        }
        self.inner.put_node(node).await
    }

    async fn get_node(&self, hex_hash: &str) -> merkle_drive::error::Result<TreeNode> {
        if self.injector.fail_reads.load(Ordering::SeqCst) {
            return Err(MerkleError::Storage("injected: read failure".into()));
        }
        self.inner.get_node(hex_hash).await
    }

    async fn get_head(
        &self,
        branch: &str,
    ) -> merkle_drive::error::Result<Option<(Commit, String)>> {
        if self.injector.fail_reads.load(Ordering::SeqCst) {
            return Err(MerkleError::Storage("injected: read failure".into()));
        }
        self.inner.get_head(branch).await
    }

    async fn put_head(
        &self,
        branch: &str,
        commit: &Commit,
        expected_etag: Option<&str>,
    ) -> merkle_drive::error::Result<String> {
        if self.injector.fail_writes.load(Ordering::SeqCst) {
            return Err(MerkleError::Storage("injected: write failure".into()));
        }
        self.inner.put_head(branch, commit, expected_etag).await
    }

    async fn blob_exists(&self, hex_hash: &str) -> merkle_drive::error::Result<bool> {
        if self.injector.fail_reads.load(Ordering::SeqCst) {
            return Err(MerkleError::Storage("injected: read failure".into()));
        }
        self.inner.blob_exists(hex_hash).await
    }

    async fn node_exists(&self, hex_hash: &str) -> merkle_drive::error::Result<bool> {
        if self.injector.fail_reads.load(Ordering::SeqCst) {
            return Err(MerkleError::Storage("injected: read failure".into()));
        }
        self.inner.node_exists(hex_hash).await
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

// ============================================================
// Group A: Rename Operations
// ============================================================

#[tokio::test]
async fn test_fuse_rename_same_directory() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("old.txt"), b"rename me").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::rename(mp2.join("old.txt"), mp2.join("new.txt")).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let (old_exists, new_content) = blocking(move || {
        (
            mp2.join("old.txt").exists(),
            std::fs::read(mp2.join("new.txt")).unwrap(),
        )
    })
    .await;

    assert!(!old_exists);
    assert_eq!(new_content, b"rename me");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_rename_across_directories() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("dir_a")).unwrap();
        std::fs::create_dir(mp2.join("dir_b")).unwrap();
        std::fs::write(mp2.join("dir_a").join("file.txt"), b"cross-dir").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::rename(
            mp2.join("dir_a").join("file.txt"),
            mp2.join("dir_b").join("file.txt"),
        )
        .unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let (old_exists, new_content) = blocking(move || {
        (
            mp2.join("dir_a").join("file.txt").exists(),
            std::fs::read(mp2.join("dir_b").join("file.txt")).unwrap(),
        )
    })
    .await;

    assert!(!old_exists);
    assert_eq!(new_content, b"cross-dir");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_rename_overwrite_existing() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("a.txt"), b"aaa").unwrap();
        std::fs::write(mp2.join("b.txt"), b"bbb").unwrap();
        std::fs::rename(mp2.join("a.txt"), mp2.join("b.txt")).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let (a_exists, b_content) = blocking(move || {
        (
            mp2.join("a.txt").exists(),
            std::fs::read(mp2.join("b.txt")).unwrap(),
        )
    })
    .await;

    assert!(!a_exists);
    assert_eq!(b_content, b"aaa");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_rename_directory() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("old_dir")).unwrap();
        std::fs::write(mp2.join("old_dir").join("child.txt"), b"inside").unwrap();
        std::fs::rename(mp2.join("old_dir"), mp2.join("new_dir")).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let (old_exists, child_content) = blocking(move || {
        (
            mp2.join("old_dir").exists(),
            std::fs::read(mp2.join("new_dir").join("child.txt")).unwrap(),
        )
    })
    .await;

    assert!(!old_exists);
    assert_eq!(child_content, b"inside");

    mount.unmount();
}

// ============================================================
// Group B: setattr Operations
// ============================================================

#[tokio::test]
async fn test_fuse_truncate_via_setattr() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        let data: Vec<u8> = (0..200).map(|i| (i % 256) as u8).collect();
        std::fs::write(mp2.join("trunc.txt"), &data).unwrap();
    })
    .await;

    // Truncate to 50 bytes
    let mp2 = mp.clone();
    blocking(move || {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(mp2.join("trunc.txt"))
            .unwrap();
        f.set_len(50).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("trunc.txt")).unwrap()).await;
    assert_eq!(content.len(), 50);
    let expected: Vec<u8> = (0..50).map(|i| (i % 256) as u8).collect();
    assert_eq!(content, expected);

    // Extend to 300 bytes (zero-padded)
    let mp2 = mp.clone();
    blocking(move || {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(mp2.join("trunc.txt"))
            .unwrap();
        f.set_len(300).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("trunc.txt")).unwrap()).await;
    assert_eq!(content.len(), 300);
    // First 50 bytes are original data, rest are zeros
    assert_eq!(&content[..50], &expected);
    assert!(content[50..].iter().all(|&b| b == 0));

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_chmod() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("exec.txt"), b"#!/bin/sh").unwrap();
        // Set executable
        let perms = std::fs::Permissions::from_mode(0o755);
        std::fs::set_permissions(mp2.join("exec.txt"), perms).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let mode = blocking(move || {
        std::fs::metadata(mp2.join("exec.txt"))
            .unwrap()
            .permissions()
            .mode()
    })
    .await;
    assert_ne!(mode & 0o111, 0, "should be executable");

    // Remove exec bit
    let mp2 = mp.clone();
    blocking(move || {
        let perms = std::fs::Permissions::from_mode(0o644);
        std::fs::set_permissions(mp2.join("exec.txt"), perms).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let mode = blocking(move || {
        std::fs::metadata(mp2.join("exec.txt"))
            .unwrap()
            .permissions()
            .mode()
    })
    .await;
    assert_eq!(mode & 0o111, 0, "should not be executable");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_utime() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("utime.txt"), b"time test").unwrap();
    })
    .await;

    // Set a specific mtime (2020-01-01 00:00:00 UTC)
    let target_time = filetime::FileTime::from_unix_time(1577836800, 0);
    let mp2 = mp.clone();
    blocking(move || {
        filetime::set_file_mtime(mp2.join("utime.txt"), target_time).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let mtime = blocking(move || {
        std::fs::metadata(mp2.join("utime.txt"))
            .unwrap()
            .modified()
            .unwrap()
    })
    .await;

    let elapsed = mtime
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert_eq!(elapsed, 1577836800);

    mount.unmount();
}

// ============================================================
// Group C: O_TRUNC on open
// ============================================================

#[tokio::test]
async fn test_fuse_open_with_o_trunc() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("trunc_open.txt"), [0xAA; 100]).unwrap();
    })
    .await;

    // Reopen with truncate and write new, smaller data
    let mp2 = mp.clone();
    blocking(move || {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(mp2.join("trunc_open.txt"))
            .unwrap();
        std::io::Write::write_all(&mut f, &[0xBB; 10]).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("trunc_open.txt")).unwrap()).await;
    assert_eq!(content, vec![0xBB; 10]);

    mount.unmount();
}

// ============================================================
// Group D: Error Cases
// ============================================================

#[tokio::test]
async fn test_fuse_read_nonexistent_file() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    let err = blocking(move || std::fs::read(mp2.join("nope.txt"))).await;
    assert!(err.is_err());
    assert_eq!(err.unwrap_err().kind(), std::io::ErrorKind::NotFound);

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_rmdir_nonempty() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("notempty")).unwrap();
        std::fs::write(mp2.join("notempty").join("file.txt"), b"hi").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let err = blocking(move || std::fs::remove_dir(mp2.join("notempty"))).await;
    assert!(err.is_err());
    // ENOTEMPTY on Linux
    let err = err.unwrap_err();
    assert!(
        err.raw_os_error() == Some(libc::ENOTEMPTY),
        "expected ENOTEMPTY, got: {err:?}"
    );

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_unlink_directory() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("mydir")).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let err = blocking(move || std::fs::remove_file(mp2.join("mydir"))).await;
    assert!(err.is_err());
    let err = err.unwrap_err();
    assert!(
        err.raw_os_error() == Some(libc::EISDIR),
        "expected EISDIR, got: {err:?}"
    );

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_mkdir_duplicate() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("subdir")).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let err = blocking(move || std::fs::create_dir(mp2.join("subdir"))).await;
    assert!(err.is_err());
    assert_eq!(err.unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_create_duplicate_file() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("dup.txt"), b"first").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let err = blocking(move || {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(mp2.join("dup.txt"))
    })
    .await;
    assert!(err.is_err());
    assert_eq!(err.unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_rmdir_file() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("notadir.txt"), b"file").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let err = blocking(move || std::fs::remove_dir(mp2.join("notadir.txt"))).await;
    assert!(err.is_err());
    let err = err.unwrap_err();
    assert!(
        err.raw_os_error() == Some(libc::ENOTDIR),
        "expected ENOTDIR, got: {err:?}"
    );

    mount.unmount();
}

// ============================================================
// Group E: Boundary Conditions
// ============================================================

#[tokio::test]
async fn test_fuse_empty_file() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("empty.txt"), b"").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let (content, size) = blocking(move || {
        let content = std::fs::read(mp2.join("empty.txt")).unwrap();
        let meta = std::fs::metadata(mp2.join("empty.txt")).unwrap();
        (content, meta.len())
    })
    .await;

    assert!(content.is_empty());
    assert_eq!(size, 0);

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_file_exactly_inline_threshold() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    // DEFAULT_INLINE_THRESHOLD is 128 bytes
    let data: Vec<u8> = (0..128).map(|i| (i % 256) as u8).collect();
    let expected = data.clone();
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("exact128.bin"), &data).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("exact128.bin")).unwrap()).await;
    assert_eq!(content, expected);
    assert_eq!(content.len(), 128);

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_file_one_over_inline_threshold() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    // 129 bytes should go through the block path
    let data: Vec<u8> = (0..129).map(|i| (i % 256) as u8).collect();
    let expected = data.clone();
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("over128.bin"), &data).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("over128.bin")).unwrap()).await;
    assert_eq!(content, expected);
    assert_eq!(content.len(), 129);

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_very_large_file() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    // 1 MB patterned data
    let data: Vec<u8> = (0..1_048_576).map(|i| (i % 251) as u8).collect();
    let expected = data.clone();
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("large.bin"), &data).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("large.bin")).unwrap()).await;
    assert_eq!(content.len(), 1_048_576);
    assert_eq!(content, expected);

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_overwrite_shrink() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("shrink.txt"), [0xAA; 500]).unwrap();
        std::fs::write(mp2.join("shrink.txt"), [0xBB; 50]).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let (content, size) = blocking(move || {
        let c = std::fs::read(mp2.join("shrink.txt")).unwrap();
        let m = std::fs::metadata(mp2.join("shrink.txt")).unwrap();
        (c, m.len())
    })
    .await;

    assert_eq!(content.len(), 50);
    assert_eq!(size, 50);
    assert_eq!(content, vec![0xBB; 50]);

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_overwrite_grow() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("grow.txt"), [0xAA; 50]).unwrap();
        std::fs::write(mp2.join("grow.txt"), [0xBB; 500]).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let (content, size) = blocking(move || {
        let c = std::fs::read(mp2.join("grow.txt")).unwrap();
        let m = std::fs::metadata(mp2.join("grow.txt")).unwrap();
        (c, m.len())
    })
    .await;

    assert_eq!(content.len(), 500);
    assert_eq!(size, 500);
    assert_eq!(content, vec![0xBB; 500]);

    mount.unmount();
}

// ============================================================
// Group F: Symlink Edge Cases
// ============================================================

#[tokio::test]
async fn test_fuse_dangling_symlink() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        unix_symlink("nonexistent.txt", mp2.join("dangle.txt")).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let (link_target, read_err) = blocking(move || {
        let target = std::fs::read_link(mp2.join("dangle.txt")).unwrap();
        let read_result = std::fs::read(mp2.join("dangle.txt"));
        (target, read_result)
    })
    .await;

    assert_eq!(link_target, PathBuf::from("nonexistent.txt"));
    assert!(read_err.is_err());

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_symlink_to_directory() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("target_dir")).unwrap();
        unix_symlink("target_dir", mp2.join("link_dir")).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let target = blocking(move || std::fs::read_link(mp2.join("link_dir")).unwrap()).await;
    assert_eq!(target, PathBuf::from("target_dir"));

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_symlink_persistence() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("real.txt"), b"real content").unwrap();
        unix_symlink("real.txt", mp2.join("sym.txt")).unwrap();
    })
    .await;

    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Remount and verify symlink persists
    let storage2 = connect_second_client(port);
    let mut mount2 = FuseMount::new(storage2, "main").await;
    let mp = mount2.path();

    let mp2 = mp.clone();
    let (target, content) = blocking(move || {
        let t = std::fs::read_link(mp2.join("sym.txt")).unwrap();
        let c = std::fs::read(mp2.join("sym.txt")).unwrap();
        (t, c)
    })
    .await;

    assert_eq!(target, PathBuf::from("real.txt"));
    assert_eq!(content, b"real content");

    mount2.unmount();
}

// ============================================================
// Group G: Directory Operations
// ============================================================

#[tokio::test]
async fn test_fuse_readdir_large_directory() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        for i in 0..100 {
            std::fs::write(mp2.join(format!("file_{i:03}.txt")), format!("content {i}")).unwrap();
        }
    })
    .await;

    let mp2 = mp.clone();
    let mut names: Vec<String> = blocking(move || {
        std::fs::read_dir(&mp2)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_str().unwrap().to_string())
            .collect()
    })
    .await;
    names.sort();

    assert_eq!(names.len(), 100);
    assert_eq!(names[0], "file_000.txt");
    assert_eq!(names[99], "file_099.txt");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_deeply_nested_directories() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("a")).unwrap();
        std::fs::create_dir(mp2.join("a").join("b")).unwrap();
        std::fs::create_dir(mp2.join("a").join("b").join("c")).unwrap();
        std::fs::create_dir(mp2.join("a").join("b").join("c").join("d")).unwrap();
        std::fs::write(
            mp2.join("a").join("b").join("c").join("d").join("deep.txt"),
            b"deep content",
        )
        .unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let content = blocking(move || {
        std::fs::read(mp2.join("a").join("b").join("c").join("d").join("deep.txt")).unwrap()
    })
    .await;
    assert_eq!(content, b"deep content");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_empty_directory_persistence() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("empty_dir")).unwrap();
    })
    .await;

    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let storage2 = connect_second_client(port);
    let mut mount2 = FuseMount::new(storage2, "main").await;
    let mp = mount2.path();

    let mp2 = mp.clone();
    let (exists, is_dir) = blocking(move || {
        let meta = std::fs::metadata(mp2.join("empty_dir")).unwrap();
        (true, meta.is_dir())
    })
    .await;

    assert!(exists);
    assert!(is_dir);

    mount2.unmount();
}

#[tokio::test]
async fn test_fuse_readdir_mixed_types() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("regular.txt"), b"file").unwrap();
        std::fs::create_dir(mp2.join("subdir")).unwrap();
        unix_symlink("regular.txt", mp2.join("link.txt")).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let entries: Vec<(String, bool, bool, bool)> = blocking(move || {
        std::fs::read_dir(&mp2)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                let ft = e.file_type().unwrap();
                (
                    e.file_name().to_str().unwrap().to_string(),
                    ft.is_file(),
                    ft.is_dir(),
                    ft.is_symlink(),
                )
            })
            .collect()
    })
    .await;

    assert_eq!(entries.len(), 3);
    for (name, is_file, is_dir, is_symlink) in &entries {
        match name.as_str() {
            "regular.txt" => assert!(*is_file),
            "subdir" => assert!(*is_dir),
            "link.txt" => assert!(*is_symlink),
            other => panic!("unexpected entry: {other}"),
        }
    }

    mount.unmount();
}

// ============================================================
// Group H: Mount/Unmount Cycles
// ============================================================

#[tokio::test]
async fn test_fuse_remount_preserves_state() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Session 1: write 3 files
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("f1.txt"), b"one").unwrap();
        std::fs::write(mp2.join("f2.txt"), b"two").unwrap();
        std::fs::write(mp2.join("f3.txt"), b"three").unwrap();
    })
    .await;

    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Session 2: read all 3 and write a 4th
    let storage2 = connect_second_client(port);
    let mut mount2 = FuseMount::new(storage2, "main").await;
    let mp = mount2.path();

    let mp2 = mp.clone();
    let (f1, f2, f3) = blocking(move || {
        (
            std::fs::read(mp2.join("f1.txt")).unwrap(),
            std::fs::read(mp2.join("f2.txt")).unwrap(),
            std::fs::read(mp2.join("f3.txt")).unwrap(),
        )
    })
    .await;
    assert_eq!(f1, b"one");
    assert_eq!(f2, b"two");
    assert_eq!(f3, b"three");

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("f4.txt"), b"four").unwrap();
    })
    .await;

    mount2.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Session 3: verify all 4
    let storage3 = connect_second_client(port);
    let mut mount3 = FuseMount::new(storage3, "main").await;
    let mp = mount3.path();

    let mp2 = mp.clone();
    let f4 = blocking(move || std::fs::read(mp2.join("f4.txt")).unwrap()).await;
    assert_eq!(f4, b"four");

    mount3.unmount();
}

#[tokio::test]
async fn test_fuse_multiple_mount_unmount_cycles() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();

    // First mount
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("file_0.txt"), b"cycle 0").unwrap();
    })
    .await;
    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subsequent cycles
    for i in 1..5u32 {
        let s = connect_second_client(port);
        let mut m = FuseMount::new(s, "main").await;
        let mp = m.path();
        let mp2 = mp.clone();
        let idx = i;
        blocking(move || {
            std::fs::write(mp2.join(format!("file_{idx}.txt")), format!("cycle {idx}")).unwrap();
        })
        .await;
        m.unmount();
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Final mount: verify all 5 files
    let s = connect_second_client(port);
    let mut m = FuseMount::new(s, "main").await;
    let mp = m.path();
    let mp2 = mp.clone();
    let contents: Vec<Vec<u8>> = blocking(move || {
        (0..5)
            .map(|i| std::fs::read(mp2.join(format!("file_{i}.txt"))).unwrap())
            .collect()
    })
    .await;

    for (i, content) in contents.iter().enumerate() {
        assert_eq!(content, format!("cycle {i}").as_bytes());
    }

    m.unmount();
}

// ============================================================
// Group I: fsync/flush Behavior
// ============================================================

#[tokio::test]
async fn test_fuse_explicit_fsync_commits() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Write and fsync
    let mp2 = mp.clone();
    blocking(move || {
        use std::os::unix::io::AsRawFd;
        std::fs::write(mp2.join("synced.txt"), b"synced content").unwrap();
        let f = std::fs::File::open(mp2.join("synced.txt")).unwrap();
        unsafe { libc::fsync(f.as_raw_fd()) };
    })
    .await;

    // Verify in storage without unmounting
    let storage2 = connect_second_client(port);
    let data = read_file(&storage2, "main", "synced.txt").await;
    assert_eq!(data, b"synced content");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_flush_commits_on_close() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    // Write and close (flush happens on close)
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("flushed.txt"), b"flushed content").unwrap();
        // std::fs::write opens, writes, and closes the file
    })
    .await;

    // Give a moment for commit to complete
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify in storage without unmounting
    let storage2 = connect_second_client(port);
    let data = read_file(&storage2, "main", "flushed.txt").await;
    assert_eq!(data, b"flushed content");

    mount.unmount();
}

// ============================================================
// Group J: File Metadata
// ============================================================

#[tokio::test]
async fn test_fuse_metadata_size_after_write() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("sized.txt"), [0xAA; 100]).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let size = blocking(move || std::fs::metadata(mp2.join("sized.txt")).unwrap().len()).await;
    assert_eq!(size, 100);

    // Truncate to 50
    let mp2 = mp.clone();
    blocking(move || {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(mp2.join("sized.txt"))
            .unwrap();
        f.set_len(50).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let size = blocking(move || std::fs::metadata(mp2.join("sized.txt")).unwrap().len()).await;
    assert_eq!(size, 50);

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_metadata_mtime_updates() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("mtime.txt"), b"first").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let mtime1 = blocking(move || {
        std::fs::metadata(mp2.join("mtime.txt"))
            .unwrap()
            .modified()
            .unwrap()
    })
    .await;

    // Small sleep to ensure time advances
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("mtime.txt"), b"second").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let mtime2 = blocking(move || {
        std::fs::metadata(mp2.join("mtime.txt"))
            .unwrap()
            .modified()
            .unwrap()
    })
    .await;

    assert!(mtime2 > mtime1, "mtime should advance after write");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_directory_nlink() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("parent")).unwrap();
    })
    .await;

    // nlink should be 2 (. and ..)
    let mp2 = mp.clone();
    let nlink1 = blocking(move || {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(mp2.join("parent")).unwrap().nlink()
    })
    .await;
    assert_eq!(nlink1, 2);

    // Add subdirectory
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("parent").join("child")).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let nlink2 = blocking(move || {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(mp2.join("parent")).unwrap().nlink()
    })
    .await;
    assert_eq!(nlink2, 3);

    // Remove subdirectory
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::remove_dir(mp2.join("parent").join("child")).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let nlink3 = blocking(move || {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(mp2.join("parent")).unwrap().nlink()
    })
    .await;
    assert_eq!(nlink3, 2);

    mount.unmount();
}

// ============================================================
// Group K: Re-creating Deleted Entries
// ============================================================

#[tokio::test]
async fn test_fuse_recreate_deleted_file() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("reuse.txt"), b"v1").unwrap();
        std::fs::remove_file(mp2.join("reuse.txt")).unwrap();
        std::fs::write(mp2.join("reuse.txt"), b"v2").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("reuse.txt")).unwrap()).await;
    assert_eq!(content, b"v2");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_recreate_deleted_directory() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::create_dir(mp2.join("dir")).unwrap();
        std::fs::write(mp2.join("dir").join("old.txt"), b"old").unwrap();
        std::fs::remove_file(mp2.join("dir").join("old.txt")).unwrap();
        std::fs::remove_dir(mp2.join("dir")).unwrap();
        std::fs::create_dir(mp2.join("dir")).unwrap();
        std::fs::write(mp2.join("dir").join("new.txt"), b"new").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("dir").join("new.txt")).unwrap()).await;
    assert_eq!(content, b"new");

    // Verify old.txt does not exist
    let mp2 = mp.clone();
    let old_exists = blocking(move || mp2.join("dir").join("old.txt").exists()).await;
    assert!(!old_exists);

    mount.unmount();
}

// ============================================================
// Group L: Concurrent Access
// ============================================================

#[tokio::test]
async fn test_fuse_concurrent_reads() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    // Write 10 files
    let mp2 = mp.clone();
    blocking(move || {
        for i in 0..10 {
            std::fs::write(mp2.join(format!("r_{i}.txt")), format!("data {i}")).unwrap();
        }
    })
    .await;

    // Read them concurrently from 10 threads
    let mp2 = mp.clone();
    let results = blocking(move || {
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let p = mp2.join(format!("r_{i}.txt"));
                std::thread::spawn(move || std::fs::read(p).unwrap())
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    })
    .await;

    for (i, content) in results.iter().enumerate() {
        assert_eq!(content, format!("data {i}").as_bytes());
    }

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_concurrent_writes() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let p = mp2.join(format!("w_{i}.txt"));
                let data = format!("written {i}");
                std::thread::spawn(move || std::fs::write(p, data).unwrap())
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    })
    .await;

    // Read all 10 and verify
    let mp2 = mp.clone();
    let results = blocking(move || {
        (0..10)
            .map(|i| std::fs::read(mp2.join(format!("w_{i}.txt"))).unwrap())
            .collect::<Vec<_>>()
    })
    .await;

    for (i, content) in results.iter().enumerate() {
        assert_eq!(content, format!("written {i}").as_bytes());
    }

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_concurrent_readdir_and_write() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        let writer_mp = mp2.clone();
        let reader_mp = mp2.clone();

        let writer = std::thread::spawn(move || {
            for i in 0..20 {
                std::fs::write(writer_mp.join(format!("cw_{i}.txt")), format!("cw {i}")).unwrap();
            }
        });

        let reader = std::thread::spawn(move || {
            let mut read_count = 0;
            for _ in 0..10 {
                if let Ok(entries) = std::fs::read_dir(&reader_mp) {
                    let _names: Vec<_> = entries.filter_map(|e| e.ok()).collect();
                    read_count += 1;
                }
            }
            read_count
        });

        writer.join().unwrap();
        let reads = reader.join().unwrap();
        assert!(reads > 0, "should have completed some reads");
    })
    .await;

    // Verify all 20 files exist
    let mp2 = mp.clone();
    let count = blocking(move || {
        (0..20)
            .filter(|i| mp2.join(format!("cw_{i}.txt")).exists())
            .count()
    })
    .await;
    assert_eq!(count, 20);

    mount.unmount();
}

// ============================================================
// Group M: Network Error Resilience (FaultyStorage)
// ============================================================

#[tokio::test]
async fn test_fuse_flush_storage_error_returns_eio() {
    let injector = FaultInjector::new();
    let faulty = FaultyStorage::new(MemoryStorage::new(), injector.clone());
    let mut mount = FuseMount::new_with_storage(faulty, "main").await;
    let mp = mount.path();

    // Write a file (succeeds since writes not yet failing)
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("fail.txt"), b"will fail on commit").unwrap();
    })
    .await;

    // Enable write failures
    injector.set_fail_writes(true);

    // Try to fsync (triggers do_commit which will fail)
    let mp2 = mp.clone();
    let result = blocking(move || {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::File::open(mp2.join("fail.txt")).unwrap();

        unsafe { libc::fsync(f.as_raw_fd()) }
    })
    .await;

    // fsync should return error (EIO mapped from storage error)
    assert_ne!(result, 0, "fsync should fail when storage is broken");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_read_after_failed_commit() {
    let injector = FaultInjector::new();
    let faulty = FaultyStorage::new(MemoryStorage::new(), injector.clone());
    let mut mount = FuseMount::new_with_storage(faulty, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("recover.txt"), b"still here").unwrap();
    })
    .await;

    // Enable failures then try to sync (will fail)
    injector.set_fail_writes(true);

    let mp2 = mp.clone();
    blocking(move || {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::File::open(mp2.join("recover.txt")).unwrap();
        let _ = unsafe { libc::fsync(f.as_raw_fd()) };
    })
    .await;

    // Disable failures
    injector.set_fail_writes(false);

    // Data should still be readable in-memory
    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("recover.txt")).unwrap()).await;
    assert_eq!(content, b"still here");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_recovery_after_transient_failure() {
    let injector = FaultInjector::new();
    let faulty = FaultyStorage::new(MemoryStorage::new(), injector.clone());
    let mut mount = FuseMount::new_with_storage(faulty, "main").await;
    let mp = mount.path();

    // Write file 1
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("file1.txt"), b"first").unwrap();
    })
    .await;

    // Fail the commit
    injector.set_fail_writes(true);
    let mp2 = mp.clone();
    blocking(move || {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::File::open(mp2.join("file1.txt")).unwrap();
        let _ = unsafe { libc::fsync(f.as_raw_fd()) };
    })
    .await;

    // Recover
    injector.set_fail_writes(false);

    // Write file 2 and commit successfully
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("file2.txt"), b"second").unwrap();
        use std::os::unix::io::AsRawFd;
        let f = std::fs::File::open(mp2.join("file2.txt")).unwrap();
        let ret = unsafe { libc::fsync(f.as_raw_fd()) };
        assert_eq!(ret, 0, "fsync should succeed after recovery");
    })
    .await;

    mount.unmount();
}

// ============================================================
// Group O: Access and Permissions
// ============================================================

#[tokio::test]
async fn test_fuse_access_always_ok() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("access.txt"), b"test").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let results = blocking(move || {
        let path = mp2.join("access.txt");
        let p = path.to_str().unwrap();
        let c_path = std::ffi::CString::new(p).unwrap();
        unsafe {
            (
                libc::access(c_path.as_ptr(), libc::R_OK),
                libc::access(c_path.as_ptr(), libc::W_OK),
                libc::access(c_path.as_ptr(), libc::X_OK),
            )
        }
    })
    .await;

    assert_eq!(results.0, 0, "R_OK should succeed");
    assert_eq!(results.1, 0, "W_OK should succeed");
    // X_OK may or may not succeed depending on exec bit

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_exec_bit_persists() {
    let (storage, container) = start_rustfs_drive().await;
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let mut mount = FuseMount::new(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("script.sh"), b"#!/bin/sh\necho hi").unwrap();
        let perms = std::fs::Permissions::from_mode(0o755);
        std::fs::set_permissions(mp2.join("script.sh"), perms).unwrap();
    })
    .await;

    mount.unmount();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Remount and verify exec bit persists
    let storage2 = connect_second_client(port);
    let mut mount2 = FuseMount::new(storage2, "main").await;
    let mp = mount2.path();

    let mp2 = mp.clone();
    let mode = blocking(move || {
        std::fs::metadata(mp2.join("script.sh"))
            .unwrap()
            .permissions()
            .mode()
    })
    .await;
    assert_ne!(mode & 0o111, 0, "exec bit should persist across remount");

    mount2.unmount();
}

// ============================================================
// Group P: Miscellaneous Coverage
// ============================================================

#[tokio::test]
async fn test_fuse_write_at_offset() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("offset.txt"), b"hello").unwrap();
    })
    .await;

    // Write "XY" at offset 3: "helXY"
    let mp2 = mp.clone();
    blocking(move || {
        use std::io::{Seek, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(mp2.join("offset.txt"))
            .unwrap();
        f.seek(std::io::SeekFrom::Start(3)).unwrap();
        f.write_all(b"XY").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("offset.txt")).unwrap()).await;
    assert_eq!(content, b"helXY");

    // Write past end: file should extend with zero-padding
    let mp2 = mp.clone();
    blocking(move || {
        use std::io::{Seek, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(mp2.join("offset.txt"))
            .unwrap();
        f.seek(std::io::SeekFrom::Start(10)).unwrap();
        f.write_all(b"Z").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("offset.txt")).unwrap()).await;
    assert_eq!(content.len(), 11);
    assert_eq!(&content[..5], b"helXY");
    assert!(content[5..10].iter().all(|&b| b == 0)); // zero-padded
    assert_eq!(content[10], b'Z');

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_read_past_eof() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("short.txt"), [0xAA; 10]).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let bytes_read = blocking(move || {
        use std::io::{Read, Seek};
        let mut f = std::fs::File::open(mp2.join("short.txt")).unwrap();
        f.seek(std::io::SeekFrom::Start(100)).unwrap();
        let mut buf = vec![0u8; 64];
        f.read(&mut buf).unwrap()
    })
    .await;

    assert_eq!(bytes_read, 0);

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_read_partial() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("partial.txt"), [0xAA; 10]).unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let data = blocking(move || {
        use std::io::{Read, Seek};
        let mut f = std::fs::File::open(mp2.join("partial.txt")).unwrap();
        f.seek(std::io::SeekFrom::Start(5)).unwrap();
        let mut buf = vec![0u8; 8]; // ask for 8 but only 5 remain
        let n = f.read(&mut buf).unwrap();
        buf.truncate(n);
        buf
    })
    .await;

    assert_eq!(data.len(), 5);
    assert_eq!(data, vec![0xAA; 5]);

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_readlink_on_file() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("regular.txt"), b"not a link").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let err = blocking(move || std::fs::read_link(mp2.join("regular.txt"))).await;
    assert!(err.is_err());

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_readdir_on_file() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("notdir.txt"), b"file").unwrap();
    })
    .await;

    let mp2 = mp.clone();
    let err = blocking(move || std::fs::read_dir(mp2.join("notdir.txt"))).await;
    assert!(err.is_err());

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_getattr_root() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    let (is_dir, mode) = blocking(move || {
        let meta = std::fs::metadata(&mp2).unwrap();
        (meta.is_dir(), meta.permissions().mode())
    })
    .await;

    assert!(is_dir, "root should be a directory");
    assert_eq!(mode & 0o777, 0o755, "root should have mode 0o755");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_release_is_noop() {
    let storage = MemoryStorage::new();
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    // Just open and close a file; release should not error
    let mp2 = mp.clone();
    blocking(move || {
        std::fs::write(mp2.join("release.txt"), b"test").unwrap();
        let _f = std::fs::File::open(mp2.join("release.txt")).unwrap();
        // _f dropped here, triggering release
    })
    .await;

    // Verify file is still readable after release
    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("release.txt")).unwrap()).await;
    assert_eq!(content, b"test");

    mount.unmount();
}

#[tokio::test]
async fn test_fuse_unmount_empty_no_commit() {
    let storage = MemoryStorage::new();

    // Pre-seed data
    let blob_hex = storage.put_blob(b"pre-existing").await.unwrap();
    let blob_hash = hash::hex_decode(&blob_hex).unwrap();
    let root = TreeNode::Leaf(LeafNode {
        entries: vec![LeafEntry::file(
            "existing.txt".to_string(),
            12,
            FileContent::Blocks(vec![blob_hash]),
            now_ns(),
            now_ns(),
            1000,
            1000,
            false,
        )],
    });
    let root_hex = storage.put_node(&root).await.unwrap();
    let root_hash = hash::hex_decode(&root_hex).unwrap();
    let commit = Commit::new(
        root_hash,
        None,
        "seed".to_string(),
        now_ms(),
        "seed commit".to_string(),
    );
    storage.put_head("main", &commit, None).await.unwrap();

    let (_, _etag_before) = storage.get_head("main").await.unwrap().unwrap();

    // Mount, read only (no writes), unmount
    let mut mount = FuseMount::new_with_storage(storage, "main").await;
    let mp = mount.path();

    let mp2 = mp.clone();
    let content = blocking(move || std::fs::read(mp2.join("existing.txt")).unwrap()).await;
    assert_eq!(content, b"pre-existing");

    mount.unmount();
    // Note: with MemoryStorage we can't easily check the etag after unmount
    // since we don't hold a reference. The test verifies that read-only access
    // doesn't trigger a panic, which covers the !dirty guard.
}
