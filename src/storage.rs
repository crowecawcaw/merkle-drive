use crate::canonical;
use crate::commit::Commit;
use crate::error::{MerkleError, Result};
use crate::hash;
use crate::tree::TreeNode;

/// Abstraction over S3 storage operations.
/// Enables testing with in-memory or local implementations.
pub trait Storage: Send + Sync {
    /// Put a content blob. Returns the hex hash key.
    async fn put_blob(&self, data: &[u8]) -> Result<String>;

    /// Get a content blob by hex hash.
    async fn get_blob(&self, hex_hash: &str) -> Result<Vec<u8>>;

    /// Put a tree node. Returns the hex hash key.
    async fn put_node(&self, node: &TreeNode) -> Result<String>;

    /// Get a tree node by hex hash.
    async fn get_node(&self, hex_hash: &str) -> Result<TreeNode>;

    /// Get the current HEAD commit for a branch. Returns (commit, etag).
    async fn get_head(&self, branch: &str) -> Result<Option<(Commit, String)>>;

    /// Update HEAD via CAS. Returns the new ETag on success.
    async fn put_head(
        &self,
        branch: &str,
        commit: &Commit,
        expected_etag: Option<&str>,
    ) -> Result<String>;

    /// Check if a blob exists.
    async fn blob_exists(&self, hex_hash: &str) -> Result<bool>;

    /// Check if a node exists.
    async fn node_exists(&self, hex_hash: &str) -> Result<bool>;
}

/// S3-backed storage implementation.
pub struct S3Storage {
    client: aws_sdk_s3::Client,
    data_bucket: String,
    metadata_bucket: String,
    /// Prefix for data objects (e.g., "blocks/").
    data_prefix: String,
    /// Prefix for node objects (e.g., "nodes/").
    node_prefix: String,
    /// Prefix for ref objects (e.g., "refs/heads/").
    ref_prefix: String,
}

impl S3Storage {
    pub fn new(client: aws_sdk_s3::Client, data_bucket: String, metadata_bucket: String) -> Self {
        Self {
            client,
            data_bucket,
            metadata_bucket,
            data_prefix: "blocks/".to_string(),
            node_prefix: "nodes/".to_string(),
            ref_prefix: "refs/heads/".to_string(),
        }
    }

    fn blob_key(&self, hex_hash: &str) -> String {
        format!("{}{}", self.data_prefix, hex_hash)
    }

    fn node_key(&self, hex_hash: &str) -> String {
        format!("{}{}", self.node_prefix, hex_hash)
    }

    fn ref_key(&self, branch: &str) -> String {
        format!("{}{}", self.ref_prefix, branch)
    }
}

impl Storage for S3Storage {
    async fn put_blob(&self, data: &[u8]) -> Result<String> {
        let hex_hash = hash::hex_encode(&hash::hash_blob(data));
        let key = self.blob_key(&hex_hash);

        self.client
            .put_object()
            .bucket(&self.data_bucket)
            .key(&key)
            .body(aws_sdk_s3::primitives::ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(|e| MerkleError::Storage(e.to_string()))?;

        Ok(hex_hash)
    }

    async fn get_blob(&self, hex_hash: &str) -> Result<Vec<u8>> {
        let key = self.blob_key(hex_hash);

        let resp = self
            .client
            .get_object()
            .bucket(&self.data_bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| MerkleError::Storage(e.to_string()))?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| MerkleError::Storage(e.to_string()))?
            .into_bytes();

        // Verify hash
        let actual_hash = hash::hex_encode(&hash::hash_blob(&bytes));
        if actual_hash != hex_hash {
            return Err(MerkleError::HashMismatch {
                expected: hex_hash.to_string(),
                actual: actual_hash,
            });
        }

        Ok(bytes.to_vec())
    }

    async fn put_node(&self, node: &TreeNode) -> Result<String> {
        let bytes = canonical::serialize_tree_node(node)?;
        let hex_hash = hash::hex_encode(&hash::hash_blob(&bytes));
        let key = self.node_key(&hex_hash);

        self.client
            .put_object()
            .bucket(&self.metadata_bucket)
            .key(&key)
            .body(aws_sdk_s3::primitives::ByteStream::from(bytes))
            .send()
            .await
            .map_err(|e| MerkleError::Storage(e.to_string()))?;

        Ok(hex_hash)
    }

    async fn get_node(&self, hex_hash: &str) -> Result<TreeNode> {
        let key = self.node_key(hex_hash);

        let resp = self
            .client
            .get_object()
            .bucket(&self.metadata_bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| MerkleError::Storage(e.to_string()))?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| MerkleError::Storage(e.to_string()))?
            .into_bytes();

        // Verify canonical form
        canonical::verify_canonical(&bytes)?;

        // Verify hash
        let actual_hash = hash::hex_encode(&hash::hash_blob(&bytes));
        if actual_hash != hex_hash {
            return Err(MerkleError::HashMismatch {
                expected: hex_hash.to_string(),
                actual: actual_hash,
            });
        }

        canonical::deserialize_tree_node(&bytes)
    }

    async fn get_head(&self, branch: &str) -> Result<Option<(Commit, String)>> {
        let key = self.ref_key(branch);

        let result = self
            .client
            .get_object()
            .bucket(&self.metadata_bucket)
            .key(&key)
            .send()
            .await;

        match result {
            Ok(resp) => {
                let etag = resp.e_tag().unwrap_or_default().to_string();
                let bytes = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| MerkleError::Storage(e.to_string()))?
                    .into_bytes();
                let commit = Commit::from_json(&bytes)?;
                Ok(Some((commit, etag)))
            }
            Err(e) => {
                let service_error = e.into_service_error();
                if service_error.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(MerkleError::Storage(service_error.to_string()))
                }
            }
        }
    }

    async fn put_head(
        &self,
        branch: &str,
        commit: &Commit,
        expected_etag: Option<&str>,
    ) -> Result<String> {
        let key = self.ref_key(branch);
        let json = commit.to_canonical_json()?;

        let mut req = self
            .client
            .put_object()
            .bucket(&self.metadata_bucket)
            .key(&key)
            .body(aws_sdk_s3::primitives::ByteStream::from(json))
            .content_type("application/json");

        if let Some(etag) = expected_etag {
            req = req.if_match(etag);
        } else {
            req = req.if_none_match("*");
        }

        let resp = req
            .send()
            .await
            .map_err(|e| MerkleError::Storage(e.to_string()))?;

        Ok(resp.e_tag().unwrap_or_default().to_string())
    }

    async fn blob_exists(&self, hex_hash: &str) -> Result<bool> {
        let key = self.blob_key(hex_hash);
        let result = self
            .client
            .head_object()
            .bucket(&self.data_bucket)
            .key(&key)
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) => {
                let service_error = e.into_service_error();
                if service_error.is_not_found() {
                    Ok(false)
                } else {
                    Err(MerkleError::Storage(service_error.to_string()))
                }
            }
        }
    }

    async fn node_exists(&self, hex_hash: &str) -> Result<bool> {
        let key = self.node_key(hex_hash);
        let result = self
            .client
            .head_object()
            .bucket(&self.metadata_bucket)
            .key(&key)
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) => {
                let service_error = e.into_service_error();
                if service_error.is_not_found() {
                    Ok(false)
                } else {
                    Err(MerkleError::Storage(service_error.to_string()))
                }
            }
        }
    }
}

/// In-memory storage for testing.
pub struct MemoryStorage {
    blobs: tokio::sync::RwLock<std::collections::HashMap<String, Vec<u8>>>,
    nodes: tokio::sync::RwLock<std::collections::HashMap<String, Vec<u8>>>,
    heads: tokio::sync::RwLock<std::collections::HashMap<String, (Vec<u8>, String)>>,
    etag_counter: std::sync::atomic::AtomicU64,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            blobs: tokio::sync::RwLock::new(std::collections::HashMap::new()),
            nodes: tokio::sync::RwLock::new(std::collections::HashMap::new()),
            heads: tokio::sync::RwLock::new(std::collections::HashMap::new()),
            etag_counter: std::sync::atomic::AtomicU64::new(1),
        }
    }

    fn next_etag(&self) -> String {
        let n = self
            .etag_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!("\"etag-{n}\"")
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    async fn put_blob(&self, data: &[u8]) -> Result<String> {
        let hex_hash = hash::hex_encode(&hash::hash_blob(data));
        self.blobs
            .write()
            .await
            .insert(hex_hash.clone(), data.to_vec());
        Ok(hex_hash)
    }

    async fn get_blob(&self, hex_hash: &str) -> Result<Vec<u8>> {
        self.blobs
            .read()
            .await
            .get(hex_hash)
            .cloned()
            .ok_or_else(|| MerkleError::NotFound(format!("blob {hex_hash}")))
    }

    async fn put_node(&self, node: &TreeNode) -> Result<String> {
        let bytes = canonical::serialize_tree_node(node)?;
        let hex_hash = hash::hex_encode(&hash::hash_blob(&bytes));
        self.nodes.write().await.insert(hex_hash.clone(), bytes);
        Ok(hex_hash)
    }

    async fn get_node(&self, hex_hash: &str) -> Result<TreeNode> {
        let bytes = self
            .nodes
            .read()
            .await
            .get(hex_hash)
            .cloned()
            .ok_or_else(|| MerkleError::NotFound(format!("node {hex_hash}")))?;
        canonical::verify_canonical(&bytes)?;
        canonical::deserialize_tree_node(&bytes)
    }

    async fn get_head(&self, branch: &str) -> Result<Option<(Commit, String)>> {
        let heads = self.heads.read().await;
        match heads.get(branch) {
            Some((json, etag)) => {
                let commit = Commit::from_json(json)?;
                Ok(Some((commit, etag.clone())))
            }
            None => Ok(None),
        }
    }

    async fn put_head(
        &self,
        branch: &str,
        commit: &Commit,
        expected_etag: Option<&str>,
    ) -> Result<String> {
        let mut heads = self.heads.write().await;
        let json = commit.to_canonical_json()?;

        match (heads.get(branch), expected_etag) {
            (None, None) => {
                let etag = self.next_etag();
                heads.insert(branch.to_string(), (json, etag.clone()));
                Ok(etag)
            }
            (Some((_, current_etag)), Some(expected)) if current_etag == expected => {
                let etag = self.next_etag();
                heads.insert(branch.to_string(), (json, etag.clone()));
                Ok(etag)
            }
            (Some((_, current_etag)), Some(expected)) => Err(MerkleError::CasConflict {
                expected: expected.to_string(),
                actual: current_etag.clone(),
            }),
            (Some((_, current_etag)), None) => Err(MerkleError::CasConflict {
                expected: "(none)".to_string(),
                actual: current_etag.clone(),
            }),
            (None, Some(expected)) => Err(MerkleError::CasConflict {
                expected: expected.to_string(),
                actual: "(none)".to_string(),
            }),
        }
    }

    async fn blob_exists(&self, hex_hash: &str) -> Result<bool> {
        Ok(self.blobs.read().await.contains_key(hex_hash))
    }

    async fn node_exists(&self, hex_hash: &str) -> Result<bool> {
        Ok(self.nodes.read().await.contains_key(hex_hash))
    }
}
