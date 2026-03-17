use serde::{Deserialize, Serialize};

use crate::error::{MerkleError, Result};
use crate::hash;

/// A commit object representing a snapshot of the file system.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Commit {
    pub client_id: String,
    pub message: String,
    pub parent: Option<String>,
    pub root: String,
    pub timestamp: u64,
    pub version: u64,
}

impl Commit {
    /// Create a new commit.
    pub fn new(
        root: [u8; 32],
        parent: Option<[u8; 32]>,
        client_id: String,
        timestamp: u64,
        message: String,
    ) -> Self {
        Self {
            client_id,
            message,
            parent: parent.map(|h| hash::hex_encode(&h)),
            root: hash::hex_encode(&root),
            timestamp,
            version: 1,
        }
    }

    /// Serialize to canonical JSON bytes (sorted keys, no whitespace).
    pub fn to_canonical_json(&self) -> Result<Vec<u8>> {
        // serde_json with sorted keys: we serialize through serde_json::Value
        // to ensure key ordering (serde_json::to_string on structs uses declaration order,
        // but our struct fields are already in alphabetical order via serde).
        let value =
            serde_json::to_value(self).map_err(|e| MerkleError::Serialization(e.to_string()))?;
        let json =
            serde_json::to_string(&value).map_err(|e| MerkleError::Serialization(e.to_string()))?;
        Ok(json.into_bytes())
    }

    /// Deserialize from JSON bytes.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| MerkleError::Serialization(e.to_string()))
    }

    /// Compute the SHA-256 hash of this commit's canonical JSON.
    pub fn hash(&self) -> Result<[u8; 32]> {
        let json = self.to_canonical_json()?;
        Ok(hash::hash_commit_json(&json))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_canonical_json() {
        let commit = Commit::new(
            [0xab; 32],
            None,
            "laptop-alice-01".to_string(),
            1742000000000,
            "initial".to_string(),
        );

        let json = commit.to_canonical_json().unwrap();
        let json_str = std::str::from_utf8(&json).unwrap();

        // Verify keys are sorted
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
        let obj = parsed.as_object().unwrap();
        let keys: Vec<&String> = obj.keys().collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        assert_eq!(keys, sorted_keys);

        // Verify null parent
        assert!(json_str.contains("\"parent\":null"));
    }

    #[test]
    fn test_commit_round_trip() {
        let commit = Commit::new(
            [0xab; 32],
            Some([0xcd; 32]),
            "client-1".to_string(),
            1742000000000,
            "update".to_string(),
        );

        let json = commit.to_canonical_json().unwrap();
        let deserialized = Commit::from_json(&json).unwrap();
        assert_eq!(commit, deserialized);
    }

    #[test]
    fn test_commit_hash_deterministic() {
        let commit = Commit::new(
            [0xab; 32],
            None,
            "client-1".to_string(),
            1742000000000,
            "".to_string(),
        );

        let hash1 = commit.hash().unwrap();
        let hash2 = commit.hash().unwrap();
        assert_eq!(hash1, hash2);
    }
}
