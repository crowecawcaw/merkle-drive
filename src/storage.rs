use crate::canonical;
use crate::commit::Commit;
use crate::error::{MerkleError, Result};
use crate::hash;
use crate::tree::TreeNode;

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

    /// Put a content blob. Returns the hex hash key.
    pub async fn put_blob(&self, data: &[u8]) -> Result<String> {
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

    /// Get a content blob by hex hash.
    pub async fn get_blob(&self, hex_hash: &str) -> Result<Vec<u8>> {
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

    /// Put a tree node. Returns the hex hash key.
    pub async fn put_node(&self, node: &TreeNode) -> Result<String> {
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

    /// Get a tree node by hex hash.
    pub async fn get_node(&self, hex_hash: &str) -> Result<TreeNode> {
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

    /// Get the current HEAD commit for a branch. Returns (commit, etag).
    pub async fn get_head(&self, branch: &str) -> Result<Option<(Commit, String)>> {
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

    /// Update HEAD via CAS. Returns the new ETag on success.
    pub async fn put_head(
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

    /// Check if a blob exists.
    pub async fn blob_exists(&self, hex_hash: &str) -> Result<bool> {
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

    /// Check if a node exists.
    pub async fn node_exists(&self, hex_hash: &str) -> Result<bool> {
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
