#![allow(dead_code)]
//! Shared test infrastructure for merkle-drive integration tests.
//!
//! Provides a single RustFS (S3-compatible) container shared across tests,
//! with per-test bucket pairs to avoid interference.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use merkle_drive::commit::Commit;
use merkle_drive::hash;
use merkle_drive::storage::S3Storage;
use merkle_drive::tree::*;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

/// Global counter for unique bucket names.
static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(1);

/// A running RustFS container with an S3 client.
pub struct RustfsContainer {
    pub port: u16,
    _container: testcontainers::ContainerAsync<GenericImage>,
    client: aws_sdk_s3::Client,
}

impl RustfsContainer {
    /// Start a single RustFS container. Call once per test file (or use lazy_static).
    pub async fn start() -> Self {
        let container = GenericImage::new("rustfs/rustfs", "latest")
            .with_exposed_port(9000.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Starting:"))
            .with_env_var("RUSTFS_ACCESS_KEY", "testuser")
            .with_env_var("RUSTFS_SECRET_KEY", "testpassword")
            .start()
            .await
            .expect("failed to start RustFS container");

        // Give RustFS a moment to fully initialize after the startup message
        tokio::time::sleep(Duration::from_secs(2)).await;

        let port = container
            .get_host_port_ipv4(9000)
            .await
            .expect("failed to get mapped port");

        let client = make_s3_client(port);

        Self {
            port,
            _container: container,
            client,
        }
    }

    /// Create a fresh pair of buckets and return an S3Storage connected to them.
    /// Each call returns unique bucket names to isolate tests.
    pub async fn new_storage(&self) -> S3Storage {
        let n = BUCKET_COUNTER.fetch_add(1, Ordering::SeqCst);
        let data_bucket = format!("data-{n}");
        let meta_bucket = format!("meta-{n}");

        self.client
            .create_bucket()
            .bucket(&data_bucket)
            .send()
            .await
            .expect("failed to create data bucket");

        self.client
            .create_bucket()
            .bucket(&meta_bucket)
            .send()
            .await
            .expect("failed to create metadata bucket");

        S3Storage::new(self.client.clone(), data_bucket, meta_bucket)
    }

    /// Create a pair of S3Storage clients pointing at the same buckets.
    pub async fn new_storage_pair(&self) -> (S3Storage, S3Storage) {
        let n = BUCKET_COUNTER.fetch_add(1, Ordering::SeqCst);
        let data_bucket = format!("data-{n}");
        let meta_bucket = format!("meta-{n}");

        self.client
            .create_bucket()
            .bucket(&data_bucket)
            .send()
            .await
            .expect("failed to create data bucket");

        self.client
            .create_bucket()
            .bucket(&meta_bucket)
            .send()
            .await
            .expect("failed to create metadata bucket");

        let client1 = make_s3_client(self.port);
        let client2 = make_s3_client(self.port);

        let s1 = S3Storage::new(client1, data_bucket.clone(), meta_bucket.clone());
        let s2 = S3Storage::new(client2, data_bucket, meta_bucket);

        (s1, s2)
    }
}

fn make_s3_client(port: u16) -> aws_sdk_s3::Client {
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
    aws_sdk_s3::Client::from_conf(config)
}

// ============================================================
// Timestamp helpers
// ============================================================

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// ============================================================
// Tree construction helpers
// ============================================================

pub fn make_leaf(entries: Vec<LeafEntry>) -> TreeNode {
    TreeNode::Leaf(LeafNode { entries })
}

pub fn inline_file(name: &str, data: &[u8]) -> LeafEntry {
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

pub fn block_file(name: &str, size: u64, block_hashes: Vec<[u8; 32]>) -> LeafEntry {
    let ts = now_ns();
    LeafEntry::file(
        name.to_string(),
        size,
        FileContent::Blocks(block_hashes),
        ts,
        ts,
        1000,
        1000,
        false,
    )
}

// ============================================================
// Storage read/write helpers
// ============================================================

/// Write a file as a blob, build a single-file root tree, commit it,
/// and return (root_hash_hex, etag).
#[allow(clippy::too_many_arguments)]
pub async fn write_single_file(
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
pub async fn read_file(storage: &S3Storage, branch: &str, filename: &str) -> Vec<u8> {
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
pub async fn read_nested_file(
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

pub struct FuseMount {
    session: Option<fuser::BackgroundSession>,
    mount_dir: tempfile::TempDir,
}

impl FuseMount {
    pub async fn new(storage: S3Storage, branch: &str) -> Self {
        use merkle_drive::fuse::MerkleFuse;

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

    pub fn path(&self) -> PathBuf {
        self.mount_dir.path().to_owned()
    }

    pub fn unmount(&mut self) {
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
pub async fn blocking<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.unwrap()
}
