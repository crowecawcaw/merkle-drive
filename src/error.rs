use thiserror::Error;

#[derive(Debug, Error)]
pub enum MerkleError {
    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("canonical verification failed: bytes do not match after round-trip")]
    CanonicalMismatch,

    #[error("unknown schema version: {0}")]
    UnknownVersion(u64),

    #[error("invalid tree node: {0}")]
    InvalidNode(String),

    #[error("entry not found: {0}")]
    NotFound(String),

    #[error("S3 error: {0}")]
    Storage(String),

    #[error("CAS conflict: expected ETag {expected}, got {actual}")]
    CasConflict { expected: String, actual: String },

    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, MerkleError>;
