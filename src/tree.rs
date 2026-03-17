use serde::{Deserialize, Serialize};

/// Schema version for tree nodes.
pub const CURRENT_VERSION: u64 = 1;

/// Default inline threshold in bytes.
pub const DEFAULT_INLINE_THRESHOLD: u64 = 128;

/// Maximum node size in bytes before splitting.
pub const MAX_NODE_SIZE: usize = 1_048_576; // 1 MiB

/// Split threshold: 75% of max node size.
pub const SPLIT_THRESHOLD: usize = MAX_NODE_SIZE * 3 / 4; // 768 KiB

/// Merge threshold: 25% of max node size.
pub const MERGE_THRESHOLD: usize = MAX_NODE_SIZE / 4; // 256 KiB

/// A tree node in the Merkle tree. Either a leaf or interior node.
#[derive(Debug, Clone, PartialEq)]
pub enum TreeNode {
    Leaf(LeafNode),
    Interior(InteriorNode),
}

/// A leaf node containing directory entries.
#[derive(Debug, Clone, PartialEq)]
pub struct LeafNode {
    pub entries: Vec<LeafEntry>,
}

/// An interior (B-tree internal) node.
#[derive(Debug, Clone, PartialEq)]
pub struct InteriorNode {
    pub keys: Vec<String>,
    /// Child tree node hashes (each 32 bytes). len == keys.len() + 1.
    pub children: Vec<[u8; 32]>,
}

/// The type of a directory entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryType {
    File,
    Dir,
    Symlink,
}

/// Content storage for a file: either inline bytes or a block list.
#[derive(Debug, Clone, PartialEq)]
pub enum FileContent {
    Inline(Vec<u8>),
    Blocks(Vec<[u8; 32]>),
}

/// A single entry in a leaf node.
#[derive(Debug, Clone, PartialEq)]
pub struct LeafEntry {
    pub name: String,
    pub entry_type: EntryType,
    /// For dir entries: hash of the child directory's root tree node.
    pub hash: Option<[u8; 32]>,
    /// Modification time (nanoseconds since epoch).
    pub mtime: Option<u64>,
    /// Change time (nanoseconds since epoch).
    pub ctime: Option<u64>,
    /// File size in bytes.
    pub size: Option<u64>,
    /// Executable bit (only meaningful for files).
    pub exec: bool,
    /// Owner user ID.
    pub uid: Option<u64>,
    /// Owner group ID.
    pub gid: Option<u64>,
    /// File content (inline or blocks).
    pub content: Option<FileContent>,
    /// Symlink target path.
    pub target: Option<String>,
}

impl LeafEntry {
    /// Create a new file entry.
    #[allow(clippy::too_many_arguments)]
    pub fn file(
        name: String,
        size: u64,
        content: FileContent,
        mtime: u64,
        ctime: u64,
        uid: u64,
        gid: u64,
        exec: bool,
    ) -> Self {
        Self {
            name,
            entry_type: EntryType::File,
            hash: None,
            mtime: Some(mtime),
            ctime: Some(ctime),
            size: Some(size),
            exec,
            uid: Some(uid),
            gid: Some(gid),
            content: Some(content),
            target: None,
        }
    }

    /// Create a new directory entry.
    pub fn dir(name: String, hash: [u8; 32]) -> Self {
        Self {
            name,
            entry_type: EntryType::Dir,
            hash: Some(hash),
            mtime: None,
            ctime: None,
            size: None,
            exec: false,
            uid: None,
            gid: None,
            content: None,
            target: None,
        }
    }

    /// Create a new symlink entry.
    pub fn symlink(
        name: String,
        target: String,
        mtime: u64,
        ctime: u64,
        uid: u64,
        gid: u64,
    ) -> Self {
        Self {
            name,
            entry_type: EntryType::Symlink,
            hash: None,
            mtime: Some(mtime),
            ctime: Some(ctime),
            size: None,
            exec: false,
            uid: Some(uid),
            gid: Some(gid),
            content: None,
            target: Some(target),
        }
    }
}

impl TreeNode {
    /// Look up an entry by name in this tree node.
    /// For leaf nodes, binary searches the entries array.
    /// For interior nodes, returns the child index to follow.
    pub fn lookup_local(&self, name: &str) -> LookupResult {
        match self {
            TreeNode::Leaf(leaf) => {
                match leaf.entries.binary_search_by(|e| e.name.as_str().cmp(name)) {
                    Ok(idx) => LookupResult::Found(leaf.entries[idx].clone()),
                    Err(_) => LookupResult::NotFound,
                }
            }
            TreeNode::Interior(interior) => {
                let child_idx = match interior.keys.binary_search_by(|k| k.as_str().cmp(name)) {
                    Ok(idx) => idx + 1,
                    Err(idx) => idx,
                };
                LookupResult::FollowChild {
                    hash: interior.children[child_idx],
                    index: child_idx,
                }
            }
        }
    }

    /// Insert an entry into a leaf node. Returns the node(s) after insertion.
    /// If the node needs to split, returns (left, separator_key, right).
    pub fn insert_entry(&mut self, entry: LeafEntry) -> crate::error::Result<Option<SplitResult>> {
        match self {
            TreeNode::Leaf(leaf) => {
                let pos = leaf
                    .entries
                    .binary_search_by(|e| e.name.as_str().cmp(entry.name.as_str()));
                match pos {
                    Ok(idx) => leaf.entries[idx] = entry,
                    Err(idx) => leaf.entries.insert(idx, entry),
                }

                let serialized_size =
                    crate::canonical::serialize_tree_node(&TreeNode::Leaf(leaf.clone()))?.len();

                if serialized_size > SPLIT_THRESHOLD {
                    let mid = leaf.entries.len() / 2;
                    let separator = leaf.entries[mid].name.clone();
                    let right_entries = leaf.entries.split_off(mid);
                    let right = TreeNode::Leaf(LeafNode {
                        entries: right_entries,
                    });
                    Ok(Some(SplitResult { separator, right }))
                } else {
                    Ok(None)
                }
            }
            TreeNode::Interior(_) => Err(crate::error::MerkleError::InvalidNode(
                "cannot insert entry into interior node directly".into(),
            )),
        }
    }

    /// Remove an entry from a leaf node by name.
    pub fn remove_entry(&mut self, name: &str) -> crate::error::Result<Option<LeafEntry>> {
        match self {
            TreeNode::Leaf(leaf) => {
                match leaf.entries.binary_search_by(|e| e.name.as_str().cmp(name)) {
                    Ok(idx) => Ok(Some(leaf.entries.remove(idx))),
                    Err(_) => Ok(None),
                }
            }
            TreeNode::Interior(_) => Err(crate::error::MerkleError::InvalidNode(
                "cannot remove entry from interior node directly".into(),
            )),
        }
    }

    /// Check if this leaf node is below the merge threshold.
    pub fn needs_merge(&self) -> crate::error::Result<bool> {
        let serialized_size = crate::canonical::serialize_tree_node(self)?.len();
        Ok(serialized_size < MERGE_THRESHOLD)
    }

    /// List all entries in a leaf node.
    pub fn entries(&self) -> Option<&[LeafEntry]> {
        match self {
            TreeNode::Leaf(leaf) => Some(&leaf.entries),
            TreeNode::Interior(_) => None,
        }
    }
}

/// Result of looking up an entry in a tree node.
#[derive(Debug)]
pub enum LookupResult {
    Found(LeafEntry),
    NotFound,
    FollowChild { hash: [u8; 32], index: usize },
}

/// Result of a node split operation.
#[derive(Debug)]
pub struct SplitResult {
    pub separator: String,
    pub right: TreeNode,
}
