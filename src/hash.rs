use sha2::{Digest, Sha256};

use crate::canonical;
use crate::error::Result;
use crate::tree::TreeNode;

/// Compute the SHA-256 hash of a tree node (from its canonical MessagePack bytes).
pub fn hash_tree_node(node: &TreeNode) -> Result<[u8; 32]> {
    let bytes = canonical::serialize_tree_node(node)?;
    Ok(sha256(&bytes))
}

/// Compute SHA-256 of raw content bytes (for blobs).
pub fn hash_blob(data: &[u8]) -> [u8; 32] {
    sha256(data)
}

/// Compute SHA-256 of canonical JSON bytes (for commits).
pub fn hash_commit_json(json_bytes: &[u8]) -> [u8; 32] {
    sha256(json_bytes)
}

/// Low-level SHA-256 computation.
fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Encode a 32-byte hash as a hex string.
pub fn hex_encode(hash: &[u8; 32]) -> String {
    hex::encode(hash)
}

/// Decode a hex string into a 32-byte hash.
pub fn hex_decode(s: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(s)
        .map_err(|e| crate::error::MerkleError::InvalidNode(format!("invalid hex: {e}")))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| crate::error::MerkleError::InvalidNode("hex string is not 32 bytes".into()))?;
    Ok(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::*;

    #[test]
    fn test_hash_deterministic() {
        let node = TreeNode::Leaf(LeafNode {
            entries: vec![LeafEntry::file(
                "test.txt".to_string(),
                5,
                FileContent::Inline(b"hello".to_vec()),
                1000,
                1000,
                1000,
                1000,
                false,
            )],
        });

        let hash1 = hash_tree_node(&node).unwrap();
        let hash2 = hash_tree_node(&node).unwrap();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_different_content_different_hash() {
        let node1 = TreeNode::Leaf(LeafNode {
            entries: vec![LeafEntry::file(
                "a.txt".to_string(),
                5,
                FileContent::Inline(b"hello".to_vec()),
                1000,
                1000,
                1000,
                1000,
                false,
            )],
        });

        let node2 = TreeNode::Leaf(LeafNode {
            entries: vec![LeafEntry::file(
                "b.txt".to_string(),
                5,
                FileContent::Inline(b"hello".to_vec()),
                1000,
                1000,
                1000,
                1000,
                false,
            )],
        });

        let hash1 = hash_tree_node(&node1).unwrap();
        let hash2 = hash_tree_node(&node2).unwrap();
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_blob_hash() {
        let data = b"hello world";
        let hash1 = hash_blob(data);
        let hash2 = hash_blob(data);
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash_blob(b"different"));
    }

    #[test]
    fn test_hex_round_trip() {
        let hash = hash_blob(b"test");
        let hex_str = hex_encode(&hash);
        assert_eq!(hex_str.len(), 64);
        let decoded = hex_decode(&hex_str).unwrap();
        assert_eq!(hash, decoded);
    }
}
