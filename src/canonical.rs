use crate::error::{MerkleError, Result};
use crate::tree::*;
use rmpv::Value;
use std::collections::BTreeMap;

/// Serialize a TreeNode to canonical MessagePack bytes.
pub fn serialize_tree_node(node: &TreeNode) -> Result<Vec<u8>> {
    let value = tree_node_to_value(node);
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &value)
        .map_err(|e| MerkleError::Serialization(e.to_string()))?;
    Ok(buf)
}

/// Deserialize canonical MessagePack bytes into a TreeNode.
pub fn deserialize_tree_node(bytes: &[u8]) -> Result<TreeNode> {
    let mut cursor = std::io::Cursor::new(bytes);
    let value = rmpv::decode::read_value(&mut cursor)
        .map_err(|e| MerkleError::Serialization(e.to_string()))?;
    value_to_tree_node(&value)
}

/// Verify that bytes are in canonical form by round-tripping.
pub fn verify_canonical(bytes: &[u8]) -> Result<()> {
    let node = deserialize_tree_node(bytes)?;
    let re_serialized = serialize_tree_node(&node)?;
    if bytes != re_serialized.as_slice() {
        return Err(MerkleError::CanonicalMismatch);
    }
    Ok(())
}

/// Convert a TreeNode to an rmpv::Value with canonical key ordering.
fn tree_node_to_value(node: &TreeNode) -> Value {
    let mut map = BTreeMap::new();

    match node {
        TreeNode::Leaf(leaf) => {
            let entries: Vec<Value> = leaf.entries.iter().map(leaf_entry_to_value).collect();
            map.insert("entries", Value::Array(entries));
            map.insert("kind", Value::String("leaf".into()));
        }
        TreeNode::Interior(interior) => {
            let children: Vec<Value> = interior
                .children
                .iter()
                .map(|h| Value::Binary(h.to_vec()))
                .collect();
            map.insert("children", Value::Array(children));
            let keys: Vec<Value> = interior
                .keys
                .iter()
                .map(|k| Value::String(k.clone().into()))
                .collect();
            map.insert("keys", Value::Array(keys));
            map.insert("kind", Value::String("interior".into()));
        }
    }

    map.insert("v", Value::Integer(CURRENT_VERSION.into()));

    // BTreeMap iterates in sorted order, which gives us canonical key ordering.
    let pairs: Vec<(Value, Value)> = map
        .into_iter()
        .map(|(k, v)| (Value::String(k.into()), v))
        .collect();

    Value::Map(pairs)
}

/// Convert a LeafEntry to an rmpv::Value with canonical key ordering.
fn leaf_entry_to_value(entry: &LeafEntry) -> Value {
    let mut map = BTreeMap::new();

    // Add fields based on entry type, sorted by key name.
    if let Some(FileContent::Blocks(blocks)) = &entry.content {
        let blocks_val: Vec<Value> = blocks.iter().map(|h| Value::Binary(h.to_vec())).collect();
        map.insert("blocks", Value::Array(blocks_val));
    }

    if let Some(ctime) = entry.ctime {
        map.insert("ctime", Value::Integer(ctime.into()));
    }

    // exec is only included when true (boolean default omission rule)
    if entry.exec {
        map.insert("exec", Value::Boolean(true));
    }

    if let Some(gid) = entry.gid {
        map.insert("gid", Value::Integer(gid.into()));
    }

    if let Some(hash) = &entry.hash {
        map.insert("hash", Value::Binary(hash.to_vec()));
    }

    if let Some(FileContent::Inline(data)) = &entry.content {
        map.insert("inline", Value::Binary(data.clone()));
    }

    if let Some(mtime) = entry.mtime {
        map.insert("mtime", Value::Integer(mtime.into()));
    }

    map.insert("name", Value::String(entry.name.clone().into()));

    if let Some(size) = entry.size {
        map.insert("size", Value::Integer(size.into()));
    }

    if let Some(target) = &entry.target {
        map.insert("target", Value::String(target.clone().into()));
    }

    let type_str = match entry.entry_type {
        EntryType::File => "file",
        EntryType::Dir => "dir",
        EntryType::Symlink => "symlink",
    };
    map.insert("type", Value::String(type_str.into()));

    if let Some(uid) = entry.uid {
        map.insert("uid", Value::Integer(uid.into()));
    }

    let pairs: Vec<(Value, Value)> = map
        .into_iter()
        .map(|(k, v)| (Value::String(k.into()), v))
        .collect();

    Value::Map(pairs)
}

/// Convert an rmpv::Value back into a TreeNode.
fn value_to_tree_node(value: &Value) -> Result<TreeNode> {
    let map = value
        .as_map()
        .ok_or_else(|| MerkleError::InvalidNode("expected map".into()))?;

    let fields = parse_map(map)?;

    let version = fields
        .get("v")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| MerkleError::InvalidNode("missing or invalid 'v' field".into()))?;

    if version != CURRENT_VERSION {
        return Err(MerkleError::UnknownVersion(version));
    }

    let kind = fields
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| MerkleError::InvalidNode("missing or invalid 'kind' field".into()))?;

    match kind {
        "leaf" => {
            let entries_val = fields
                .get("entries")
                .and_then(|v| v.as_array())
                .ok_or_else(|| MerkleError::InvalidNode("missing 'entries' array".into()))?;

            let entries: Result<Vec<LeafEntry>> =
                entries_val.iter().map(value_to_leaf_entry).collect();
            Ok(TreeNode::Leaf(LeafNode { entries: entries? }))
        }
        "interior" => {
            let keys_val = fields
                .get("keys")
                .and_then(|v| v.as_array())
                .ok_or_else(|| MerkleError::InvalidNode("missing 'keys' array".into()))?;

            let children_val = fields
                .get("children")
                .and_then(|v| v.as_array())
                .ok_or_else(|| MerkleError::InvalidNode("missing 'children' array".into()))?;

            let keys: Result<Vec<String>> = keys_val
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| MerkleError::InvalidNode("invalid key string".into()))
                })
                .collect();

            let children: Result<Vec<[u8; 32]>> = children_val
                .iter()
                .map(|v| {
                    let bytes = v
                        .as_slice()
                        .ok_or_else(|| MerkleError::InvalidNode("invalid child hash".into()))?;
                    let arr: [u8; 32] = bytes
                        .try_into()
                        .map_err(|_| MerkleError::InvalidNode("child hash not 32 bytes".into()))?;
                    Ok(arr)
                })
                .collect();

            Ok(TreeNode::Interior(InteriorNode {
                keys: keys?,
                children: children?,
            }))
        }
        other => Err(MerkleError::InvalidNode(format!("unknown kind: {other}"))),
    }
}

/// Parse a MessagePack map into a BTreeMap for easier field access.
fn parse_map(pairs: &[(Value, Value)]) -> Result<BTreeMap<&str, &Value>> {
    let mut map = BTreeMap::new();
    for (k, v) in pairs {
        let key = k
            .as_str()
            .ok_or_else(|| MerkleError::InvalidNode("map key is not a string".into()))?;
        map.insert(key, v);
    }
    Ok(map)
}

/// Parse a single leaf entry from an rmpv::Value.
fn value_to_leaf_entry(value: &Value) -> Result<LeafEntry> {
    let map = value
        .as_map()
        .ok_or_else(|| MerkleError::InvalidNode("entry is not a map".into()))?;

    let fields = parse_map(map)?;

    let name = fields
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| MerkleError::InvalidNode("missing 'name'".into()))?
        .to_string();

    let type_str = fields
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| MerkleError::InvalidNode("missing 'type'".into()))?;

    let entry_type = match type_str {
        "file" => EntryType::File,
        "dir" => EntryType::Dir,
        "symlink" => EntryType::Symlink,
        other => {
            return Err(MerkleError::InvalidNode(format!(
                "unknown entry type: {other}"
            )))
        }
    };

    let hash = fields.get("hash").and_then(|v| v.as_slice()).and_then(|b| {
        let arr: [u8; 32] = b.try_into().ok()?;
        Some(arr)
    });

    let mtime = fields.get("mtime").and_then(|v| v.as_u64());
    let ctime = fields.get("ctime").and_then(|v| v.as_u64());
    let size = fields.get("size").and_then(|v| v.as_u64());
    let exec = fields
        .get("exec")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let uid = fields.get("uid").and_then(|v| v.as_u64());
    let gid = fields.get("gid").and_then(|v| v.as_u64());
    let target = fields
        .get("target")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let content = if let Some(inline_val) = fields.get("inline") {
        Some(FileContent::Inline(
            inline_val
                .as_slice()
                .ok_or_else(|| MerkleError::InvalidNode("invalid inline content".into()))?
                .to_vec(),
        ))
    } else if let Some(blocks_val) = fields.get("blocks") {
        let blocks_arr = blocks_val
            .as_array()
            .ok_or_else(|| MerkleError::InvalidNode("blocks is not an array".into()))?;
        let blocks: Result<Vec<[u8; 32]>> = blocks_arr
            .iter()
            .map(|v| {
                let bytes = v
                    .as_slice()
                    .ok_or_else(|| MerkleError::InvalidNode("invalid block hash".into()))?;
                let arr: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| MerkleError::InvalidNode("block hash not 32 bytes".into()))?;
                Ok(arr)
            })
            .collect();
        Some(FileContent::Blocks(blocks?))
    } else {
        None
    };

    Ok(LeafEntry {
        name,
        entry_type,
        hash,
        mtime,
        ctime,
        size,
        exec,
        uid,
        gid,
        content,
        target,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node_round_trip() {
        let node = TreeNode::Leaf(LeafNode {
            entries: vec![
                LeafEntry::file(
                    ".gitignore".to_string(),
                    30,
                    FileContent::Inline(b"*.o\n*.so\ntarget/\n".to_vec()),
                    1742000000000000000,
                    1742000000000000000,
                    1000,
                    1000,
                    false,
                ),
                LeafEntry::file(
                    "README.md".to_string(),
                    200,
                    FileContent::Blocks(vec![[0xab; 32]]),
                    1742000000000000000,
                    1742000000000000000,
                    1000,
                    1000,
                    false,
                ),
                LeafEntry::dir("src".to_string(), [0xcd; 32]),
            ],
        });

        let bytes = serialize_tree_node(&node).unwrap();
        let deserialized = deserialize_tree_node(&bytes).unwrap();
        assert_eq!(node, deserialized);
    }

    #[test]
    fn test_canonical_verification() {
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

        let bytes = serialize_tree_node(&node).unwrap();
        verify_canonical(&bytes).unwrap();
    }

    #[test]
    fn test_canonical_key_ordering() {
        let node = TreeNode::Leaf(LeafNode {
            entries: vec![LeafEntry::file(
                "test.txt".to_string(),
                5,
                FileContent::Inline(b"hello".to_vec()),
                1000,
                1000,
                1000,
                1000,
                true,
            )],
        });

        let bytes = serialize_tree_node(&node).unwrap();

        // Verify key order by deserializing to raw Value
        let mut cursor = std::io::Cursor::new(&bytes);
        let value = rmpv::decode::read_value(&mut cursor).unwrap();
        let map = value.as_map().unwrap();

        // Top-level keys should be: "entries" < "kind" < "v"
        let keys: Vec<&str> = map.iter().map(|(k, _)| k.as_str().unwrap()).collect();
        assert_eq!(keys, vec!["entries", "kind", "v"]);
    }

    #[test]
    fn test_interior_node_round_trip() {
        let node = TreeNode::Interior(InteriorNode {
            keys: vec!["m".to_string()],
            children: vec![[0xaa; 32], [0xbb; 32]],
        });

        let bytes = serialize_tree_node(&node).unwrap();
        let deserialized = deserialize_tree_node(&bytes).unwrap();
        assert_eq!(node, deserialized);
        verify_canonical(&bytes).unwrap();
    }

    #[test]
    fn test_exec_omitted_when_false() {
        let entry = LeafEntry::file(
            "script.sh".to_string(),
            100,
            FileContent::Inline(b"#!/bin/sh".to_vec()),
            1000,
            1000,
            1000,
            1000,
            false,
        );

        let node = TreeNode::Leaf(LeafNode {
            entries: vec![entry],
        });

        let bytes = serialize_tree_node(&node).unwrap();

        // Parse raw and check that "exec" key is absent
        let mut cursor = std::io::Cursor::new(&bytes);
        let value = rmpv::decode::read_value(&mut cursor).unwrap();
        let entries = value.as_map().unwrap()[0].1.as_array().unwrap();
        let entry_map = entries[0].as_map().unwrap();
        let has_exec = entry_map.iter().any(|(k, _)| k.as_str() == Some("exec"));
        assert!(!has_exec, "exec should be omitted when false");
    }

    #[test]
    fn test_exec_present_when_true() {
        let entry = LeafEntry::file(
            "script.sh".to_string(),
            100,
            FileContent::Inline(b"#!/bin/sh".to_vec()),
            1000,
            1000,
            1000,
            1000,
            true,
        );

        let node = TreeNode::Leaf(LeafNode {
            entries: vec![entry],
        });

        let bytes = serialize_tree_node(&node).unwrap();

        let mut cursor = std::io::Cursor::new(&bytes);
        let value = rmpv::decode::read_value(&mut cursor).unwrap();
        let entries = value.as_map().unwrap()[0].1.as_array().unwrap();
        let entry_map = entries[0].as_map().unwrap();
        let has_exec = entry_map.iter().any(|(k, _)| k.as_str() == Some("exec"));
        assert!(has_exec, "exec should be present when true");
    }

    #[test]
    fn test_symlink_entry_round_trip() {
        let node = TreeNode::Leaf(LeafNode {
            entries: vec![LeafEntry::symlink(
                "link".to_string(),
                "/usr/bin/target".to_string(),
                1000,
                1000,
                0,
                0,
            )],
        });

        let bytes = serialize_tree_node(&node).unwrap();
        let deserialized = deserialize_tree_node(&bytes).unwrap();
        assert_eq!(node, deserialized);
        verify_canonical(&bytes).unwrap();
    }
}
