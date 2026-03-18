# S3-Backed Virtual File System — Design Document

## 1. Overview

Merkle-drive is a virtual file system backed by S3. There is no server — clients read and write to S3 directly. Content is stored in content-addressed storage (SHA-256). File structure metadata is represented as a Merkle tree, where tree node objects are themselves content-addressed and stored in S3.

The system is optimized for high read throughput with many clients. Writes are asynchronous. Conflicts on concurrent writes to the same file are expected to be rare; last writer wins.

### Interfaces

- FUSE interface first, WebDAV later.
- Core logic is abstracted from the specific file system interface so it can be reused.

### Storage layout

- **Data URI:** e.g. `s3://my-bucket/blocks/` — content-addressed file chunks.
- **Metadata URI:** e.g. `s3://my-other-bucket/metadata/` — tree nodes and commit refs.
- Data and metadata may live in different buckets.

---

## 2. Concepts Glossary

**Blob.** A content-addressed chunk of file data stored in S3. Its key is the hex-encoded SHA-256 of its raw bytes. Maximum size equals the configured chunk size (default 8 MiB).

**Block List.** An ordered list of blob hashes that, when the blobs are concatenated, yield the full content of a file.

**CAS (Compare-and-Swap).** An atomic conditional write to S3 using `If-Match` on ETag. Used to update the HEAD pointer without races — the write only succeeds if the current object matches the expected ETag.

**Chunk.** A contiguous segment of a file's content, produced by splitting the file at chunk boundaries. Each chunk is stored as a blob.

**Commit.** An immutable snapshot record stored at a mutable S3 key. Contains the root tree node hash, a parent commit hash, client identity, and timestamp. Commits form a singly-linked list (the commit chain).

**Content-Addressed.** An object whose storage key is the cryptographic hash of its contents. Content-addressed objects are immutable and naturally deduplicated.

**Fan-out.** The branching factor of the B-tree. Not a fixed constant — determined dynamically by the node size threshold.

**HEAD Pointer.** A mutable S3 object at a well-known key (e.g. `refs/heads/main`) that points to the latest commit. The only mutable object in the system. Updated via CAS.

**Inline Content.** For files at or below the inline threshold (default 128 bytes), the raw bytes stored directly in the directory entry instead of a separate blob. Avoids an extra S3 round-trip for tiny files.

**Interior Node.** A B-tree internal node for a single large directory. Contains separator keys and child pointers (tree node hashes). Used only when a directory is too large to fit in a single leaf node.

**Leaf Entry.** An entry within a leaf node that describes a file, symlink, or subdirectory. Carries name, type, and (for files/symlinks) full metadata.

**Leaf Node.** A B-tree leaf node containing the actual directory entries — file metadata, symlink metadata, and subdirectory pointers.

**Merkle Tree.** A tree data structure where every node is labeled with the hash of its contents. Changing any entry changes the hashes of all ancestors up to the root, enabling efficient change detection and integrity verification.

**Split / Merge.** When a tree node's serialized size exceeds the split threshold (75% of max node size), it splits into two nodes and promotes a separator key to the parent. When it drops below the merge threshold (25%), it merges with a sibling.

**Tree Node.** A content-addressed object representing one node of the B-tree that encodes directory structure. Stored in S3 under its SHA-256 hash. Contains either directory entries (leaf) or separator keys with child pointers (interior).

---

## 3. Serialization Format: MessagePack (Canonical Subset)

### Choice

MessagePack with a strict canonical encoding profile. Rust implementation via `rmp-serde`.

Tree nodes are serialized as MessagePack maps with string keys. This is a schemaless format — the structure is defined by convention and enforced by application code, not by a compiled schema.

### Canonical encoding rules

MessagePack does not mandate canonical encoding by default. We define a strict canonical subset that ensures the same logical content always produces the same bytes:

| Rule | Description |
|---|---|
| Map key ordering | All map keys sorted by raw UTF-8 byte comparison. No duplicate keys. |
| Minimal integer encoding | Use the smallest MessagePack integer type that fits the value. (This is default behavior for conformant encoders including `rmp`.) |
| Minimal string/bin length encoding | Use the shortest length prefix that fits. (Default `rmp` behavior.) |
| `str` vs `bin` type distinction | Field names and entry names use `str`. Hashes, inline content, and other raw bytes use `bin`. |
| No extension types | Extension types (`ext`, `fixext`) are not used anywhere in the schema. |
| No floating point | All numeric fields are integers. Floats are not used and must not appear. |
| Boolean default omission | Fields with boolean `false` values are omitted from the map to save space. Readers treat absent boolean fields as `false`. |

### Enforcing canonical order in Rust

Serializing via `rmp-serde` with `serde::Serialize` on a struct emits fields in declaration order, not as a sorted map. To guarantee sorted keys, tree nodes are serialized through a `BTreeMap<&str, Value>` intermediary, which iterates in sorted key order. Alternatively, a custom `Serialize` implementation can emit map entries in the required order directly.

### Canonical verification

A `verify_canonical(bytes) -> Result<()>` function provides the safety net:

1. Deserialize the bytes into a `TreeNode`.
2. Re-serialize the `TreeNode` to canonical MessagePack bytes.
3. Assert byte-equality with the input.

This is run on every object fetched from S3 before trusting its hash. If verification fails, the object is treated as corrupt.

### Why MessagePack

- **Compactness.** Binary format. 32-byte hashes are stored as `bin 32` (34 bytes with header) rather than 64-character hex strings. Integers use varint encoding. Significantly smaller than JSON for metadata-heavy nodes.
- **Simplicity.** No schema compiler, no code generation, no build step. The data structures are defined in Rust and serialized directly via `serde`.
- **Rust ecosystem.** `rmp-serde` is mature, actively maintained, and integrates with the serde ecosystem. Works with `#[derive(Serialize, Deserialize)]`.
- **Debuggability.** Not human-readable on disk, but `msgpack2json` CLI tools exist. We also provide a `merkle-drive dump <hash>` debug command that pretty-prints any object as JSON.
- **Schema evolution.** Map-based encoding means new fields can be added without breaking old readers — unknown keys are skipped during deserialization (configurable via `serde(deny_unknown_fields)` or not). The `v` field on every node allows readers to distinguish format versions.

### Why not the alternatives

- **Canonical JSON.** Too wasteful — 32-byte hashes become 64 hex characters plus quotes. For large directories, the overhead is significant against the 1 MiB node size budget.
- **Protobuf.** Serialization is [explicitly not canonical by spec](https://protobuf.dev/programming-guides/serialization-not-canonical/). Would require self-enforcing determinism rules on top of a format whose spec says not to rely on determinism.
- **DAG-CBOR.** Canonicality is baked into the spec (an advantage), but the Rust ecosystem (`ciborium`) is less mature than `rmp-serde`, and we don't benefit from IPLD interoperability.
- **FlatBuffers / Cap'n Proto.** Zero-copy reads are appealing, but alignment and padding rules make determinism fragile across library versions.
- **Custom binary format.** Maximum control, but the maintenance cost of a hand-rolled parser/serializer is not justified.

---

## 4. Data Structures

Every tree node is serialized as a MessagePack map at the top level. Field names are kept short (1-3 characters) to minimize overhead since they are repeated in every object.

### 4.1 TreeNode (top level)

Every object in the `nodes/` prefix is a TreeNode. The top-level map:

| Key | Type | Description |
|---|---|---|
| `"v"` | uint | Schema version. Currently `1`. Readers reject unknown versions. |
| `"kind"` | str | One of `"leaf"` or `"interior"`. |
| `"entries"` | array of maps | Present when `kind` is `"leaf"`. Sorted by `"name"`. |
| `"keys"` | array of str | Present when `kind` is `"interior"`. N sorted separator keys. |
| `"children"` | array of bin | Present when `kind` is `"interior"`. N+1 child hashes (32 bytes each). |

A node is either a leaf or an interior — it has either `entries`, or `keys` + `children`, never both.

### 4.2 Leaf entry (element of `"entries"`)

Each entry in a leaf node is a map describing one directory child:

| Key | Type | Present for | Description |
|---|---|---|---|
| `"name"` | str | all | Entry name. UTF-8, no `/` or NUL. |
| `"type"` | str | all | One of: `"file"`, `"dir"`, `"symlink"`. |
| `"hash"` | bin (32 bytes) | dir | Tree node hash of the child directory's root node. |
| `"mtime"` | uint | file, symlink | Modification time, nanoseconds since Unix epoch. |
| `"ctime"` | uint | file, symlink | Change time, nanoseconds since Unix epoch. |
| `"size"` | uint | file | File size in bytes. |
| `"exec"` | bool | file (if true) | Executable bit. **Omitted when false** (default). |
| `"uid"` | uint | file, symlink | Owner user ID. |
| `"gid"` | uint | file, symlink | Owner group ID. |
| `"blocks"` | array of bin (32B) | file, size > threshold | Ordered blob hashes forming the file content. |
| `"inline"` | bin | file, size <= threshold | Raw file content stored directly. |
| `"target"` | str | symlink | Symlink target path. |
| `"codec"` | str | file (optional) | Compression codec (e.g. `"zstd"`). Omitted = uncompressed. Reserved. |
| `"xattrs"` | map (str → bin) | optional | Extended attributes. Omitted if empty. Keys sorted. Reserved. |

**Field presence by entry type:**

| Key | `"file"` | `"dir"` | `"symlink"` |
|---|---|---|---|
| `name`, `type` | yes | yes | yes |
| `hash` | — | yes | — |
| `mtime`, `ctime` | yes | — | yes |
| `size` | yes | — | — |
| `exec` | if true | — | — |
| `uid`, `gid` | yes | — | yes |
| `blocks` or `inline` | yes | — | — |
| `target` | — | — | yes |

### 4.3 Interior node

An interior node uses the standard B-tree representation:

- `"keys"`: N separator key strings, sorted lexicographically.
- `"children"`: N+1 child tree node hashes (each exactly 32 bytes).
- `children[i]` covers entry names in the range `[keys[i-1], keys[i])`.
- `children[0]` covers names `< keys[0]`.
- `children[N]` covers names `>= keys[N-1]`.

Invariant: `len(children) == len(keys) + 1`.

### 4.4 Canonical key ordering example

Because map keys are sorted by raw byte comparison, a leaf node's top-level keys appear in this order:

```
"entries" < "kind" < "v"
```

Within a file entry, keys appear as:

```
"blocks" < "ctime" < "gid" < "mtime" < "name" < "size" < "type" < "uid"
```

(The `"exec"` key, if present, sorts between `"ctime"` and `"gid"`.)

### 4.5 Design notes

**Short key names.** Keys like `"v"`, `"uid"`, `"gid"`, `"exec"` are deliberately short. In a schemaless binary format, key strings are repeated in every entry. Short names save significant space across thousands of entries in a large directory node.

**Directory metadata.** A `"dir"` entry carries only `"name"`, `"type"`, and `"hash"`. Directories do not store permissions or timestamps in the parent's entry. If directory-level metadata is needed in the future, a `"meta"` field can be added to the top-level TreeNode map.

**Timestamps.** Nanosecond `uint64` since Unix epoch. Matches Linux `struct timespec`, avoids floating-point non-determinism, and provides sufficient range (~584 years from epoch).

**Inline threshold.** Files at or below 128 bytes (configurable) use `"inline"` instead of `"blocks"`. The threshold is a system configuration parameter, not stored in the node — a reader handles whichever field is present.

**Omitting default booleans.** `"exec"` is omitted when `false` rather than included as `false`. This saves bytes in the common case (most files are not executable). The canonical rule is: if a boolean field has its default value, omit it from the map. Readers treat absent boolean fields as `false`.

---

## 5. B-tree Structure for Large Directories

A single directory's entries are organized as a B-tree. The root of this B-tree is what a parent directory's `"hash"` field points to.

### Small directories (common case)

A directory with a modest number of entries fits entirely in a single leaf node (well under 1 MiB serialized). No interior nodes are needed. The `"hash"` points directly to this leaf.

### Large directories

When a leaf node's serialized size exceeds **768 KiB** (75% of the 1 MiB threshold) after an insertion:

1. The entries array splits at the median key into two new leaf nodes.
2. The median key is promoted into a parent interior node.
3. If no parent exists (the root was a leaf), a new interior root is created with one key and two children.
4. If the parent interior node itself exceeds the size threshold, it splits recursively upward.

When a leaf node's serialized size drops below **256 KiB** (25% of 1 MiB) after a deletion:

1. The node attempts to merge with an adjacent sibling.
2. If the merged result would exceed 768 KiB, entries are redistributed (rotation) instead of merging.

The 75%/25% split prevents flapping — a node that just split won't immediately merge, and vice versa.

### Fan-out estimate

An interior node entry consists of roughly: one key (average filename ~20 bytes) + one 32-byte child hash + MessagePack overhead ~6 bytes ≈ 58 bytes per entry. At the 768 KiB split threshold, one interior node holds approximately `768,000 / 58 ≈ 13,200` keys with 13,201 children.

For leaf entries, a typical file entry with metadata and one block hash is ~130 bytes (keys + values + msgpack overhead). A leaf node holds approximately `768,000 / 130 ≈ 5,900` entries.

A two-level tree (one interior root + leaf children) supports `13,200 × 5,900 ≈ 78 million` entries. Three levels would only be needed for astronomically large directories.

### Lookup

To find entry `"foo.txt"` in a directory:

1. Fetch the directory's root tree node by hash.
2. If it's a leaf node, binary-search `entries` for `"name" == "foo.txt"`.
3. If it's an interior node, binary-search `keys` to find the correct child index, fetch that child by hash, and repeat.

Each step is one S3 GET (likely cached locally). Tree depth is O(log₁₃₂₀₀(N)) — at most 2 S3 round-trips for any realistic directory.

---

## 6. Hash Computation

### Tree nodes

```
node_hash = SHA-256( canonical_msgpack_bytes( TreeNode ) )
```

Serialize the TreeNode to bytes following the canonical MessagePack rules from section 3, then compute SHA-256 over those bytes. The resulting 32-byte digest is the node's content address, stored hex-encoded as its S3 key.

**What gets hashed — precisely:** The entire canonical MessagePack map, including the version field, kind, and all entries/keys/children. For a leaf node, the sorted top-level keys are `"entries"`, `"kind"`, `"v"` — so the map is serialized in that key order.

### Content blobs

```
blob_hash = SHA-256( raw_chunk_bytes )
```

No envelope, no framing — just the raw chunk data. The same file content always produces the same blob hashes regardless of metadata.

### Commits

```
commit_hash = SHA-256( canonical_json_bytes( commit ) )
```

Canonical JSON with sorted keys and no whitespace. See section 7.

### Why hash the serialized form

Hashing the serialized bytes (rather than a separate canonical abstract form) is simpler and avoids a second serialization step. The canonical encoding rules guarantee that logically equivalent objects produce identical bytes, making this safe.

---

## 7. Commit Object

The commit is the only mutable object in the system. It is stored at a well-known S3 key (e.g. `refs/heads/main`) and updated via CAS.

Commits use **canonical JSON** (not MessagePack) because they are small, infrequently written, and benefit from human readability when debugging.

### Structure

```json
{
  "client_id": "laptop-alice-01",
  "message": "",
  "parent": "a1b2c3d4e5f6...64 hex chars",
  "root": "f6e5d4c3b2a1...64 hex chars",
  "timestamp": 1742000000000,
  "version": 1
}
```

| Field | Type | Description |
|---|---|---|
| `version` | integer | Commit format version. Currently `1`. |
| `root` | string (64 hex chars) | SHA-256 hash of the root directory's tree node. |
| `parent` | string or `null` | Hash of the previous commit. `null` for the initial commit. |
| `client_id` | string | Identifier of the client that created this commit. |
| `timestamp` | integer | Milliseconds since Unix epoch. |
| `message` | string | Human-readable description. May be empty. |

### Canonical JSON rules

- Keys sorted lexicographically.
- No extraneous whitespace.
- No trailing commas.
- `null` for null values (not omitted).
- Numbers with no unnecessary leading zeros or trailing decimal points.

### Commit chain

Commits form a singly-linked list via `parent`. To reconstruct history, follow parent pointers from HEAD. There is no separate log object.

The `parent` hash is computed as `SHA-256(canonical_json_bytes(parent_commit))`, creating a hash chain analogous to git commits.

---

## 8. S3 Key Layout

| Prefix | Content | Mutability |
|---|---|---|
| `blocks/<hex_hash>` | Content blobs (file chunks) | Immutable, write-once |
| `nodes/<hex_hash>` | Tree nodes (directory structure) | Immutable, write-once |
| `refs/heads/<name>` | Commit JSON | Mutable (CAS) |

Blobs and tree nodes are stored under separate prefixes. This provides:
- Clear separation for debugging and operational tooling.
- Potential for different S3 lifecycle policies (e.g. tree nodes may be GC'd more aggressively than content blobs).
- Both are content-addressed and immutable after creation.

---

## 9. Worked Example

Consider a directory `/project` containing:

```
.gitignore   (30 bytes — below inline threshold)
README.md    (200 bytes — one chunk)
src/         (subdirectory)
```

### Tree node for `/project` (leaf)

Shown as JSON-like pseudocode (actual format is canonical MessagePack):

```
{
  "entries": [
    {
      "ctime": 1742000000000000000,
      "gid": 1000,
      "inline": <30 raw bytes>,
      "mtime": 1742000000000000000,
      "name": ".gitignore",
      "size": 30,
      "type": "file",
      "uid": 1000
    },
    {
      "blocks": [<32-byte hash>],
      "ctime": 1742000000000000000,
      "gid": 1000,
      "mtime": 1742000000000000000,
      "name": "README.md",
      "size": 200,
      "type": "file",
      "uid": 1000
    },
    {
      "hash": <32-byte hash of src's root tree node>,
      "name": "src",
      "type": "dir"
    }
  ],
  "kind": "leaf",
  "v": 1
}
```

Note:
- All map keys are sorted lexicographically at every level.
- Entries are sorted by `"name"`: `.gitignore` < `README.md` < `src`.
- `.gitignore` uses `"inline"` (30 bytes < 128 byte threshold).
- `README.md` uses `"blocks"` with a single blob hash.
- `src` is a `"dir"` entry with only `"hash"`, `"name"`, and `"type"`.
- `"exec"` is absent (defaults to `false`).

This node serializes to ~280 bytes of MessagePack. The SHA-256 of those bytes becomes the hash that the parent directory (or the commit's `root` field) references.

### Corresponding commit

```json
{"client_id":"laptop-alice-01","message":"initial","parent":null,"root":"<64 hex chars>","timestamp":1742000000000,"version":1}
```

---

## 10. Write Path Summary

1. FUSE layer accepts writes and saves files to a local staging area immediately.
2. Data chunking and upload to S3 happens continuously in the background.
3. Merkle tree updates are batched on a configurable interval (default 30s) or triggered manually.
4. When updating the tree:
   - Compute new tree nodes bottom-up (new content hashes propagate to new parent hashes up to a new root).
   - Upload all new tree node objects (immutable, content-addressed) to S3.
   - Upload the new commit JSON to the HEAD key using CAS (`If-Match` on the expected ETag).
5. On CAS failure (another client updated HEAD first):
   - Fetch the new HEAD and its tree.
   - Rebase local changes onto the new tree.
   - Retry (exponential backoff, default 5 attempts).

---

## 11. Read Path Summary

1. Clients poll HEAD at a configurable interval using conditional GET (`If-None-Match` on ETag).
2. If HEAD hasn't changed (304 Not Modified), no work.
3. If HEAD changed, walk the new tree — any node whose hash matches a locally cached node is a cache hit (no fetch needed).
4. FUSE serves stale data from the local tree while a background poll refreshes. Reads never block on sync.

---

## 12. Local Read Cache

- Clients maintain a local disk cache of content blobs and tree nodes, keyed by content hash.
- LRU eviction, bounded by a configurable max size.
- Content-addressed keys provide deduplication for free.

---

## 13. Unsupported Operations

- **Hard links:** return `ENOTSUP`.
- **Extended attributes:** return `ENOTSUP` (schema field reserved for future support).

---

## 14. Open Questions

- **Garbage collection.** Old tree nodes and unreferenced content blobs accumulate. A GC strategy is needed (mark-and-sweep from live commit chain, or reference counting). Deferred.
- **Failure mode after CAS retry exhaustion.** What happens when all retries fail? Options: queue locally for next sync, return `EIO`, flag mount as degraded. Deferred.
- **Blob vs. node disambiguation.** Currently blobs and tree nodes are in separate S3 prefixes, which provides implicit typing. An alternative is a single `objects/` prefix with a type-byte prefix inside the content before hashing. The current design uses separate prefixes for simplicity.
- **Directory metadata.** Directories currently carry no permissions or timestamps. If needed, a `"meta"` field can be added to the TreeNode map.
- **Schema version migration.** When version 2 is introduced, old nodes remain readable — unknown fields are skipped by `rmp-serde` deserialization. The `"v"` field lets readers distinguish format versions and apply defaults for newly added fields.

---

## 15. Assumptions

- **S3 strong read-after-write consistency** is required. The CAS-on-HEAD design depends on it. S3-compatible stores without this guarantee (some MinIO configurations, older Ceph RGW) will not work correctly.
- **S3 conditional writes** (`If-Match` / `If-None-Match`) are available. Required for CAS on the HEAD pointer.

---

## 16. Implementation Notes

- **Language:** Rust.
- **Core library** for business logic, reusable across FUSE and WebDAV interfaces.
- **FUSE interface** first, WebDAV second.
- **Key crates:** `rmp-serde` for MessagePack, `sha2` for SHA-256, `serde` + `serde_json` for commits.
- **Testing:** Unit tests for business logic (chunking, tree operations, serialization canonicality round-trip). Integration tests: mount, read/write files, unmount, remount, verify. Multi-client concurrency tests.
- **Local S3:** Rustfs (or LocalStack) as the S3 endpoint for integration tests.
