# Integration Test Plan: merkle-drive

## Architecture

### Code Reuse: `tests/common/mod.rs`

Extract all duplicated helpers from `rustfs_integ_test.rs` and `fuse_integ_test.rs`:
- `start_rustfs_drive()`, `connect_second_client(port)`
- `now_ms()`, `now_ns()`
- `make_leaf()`, `inline_file()`, `block_file()`
- `read_file()`, `read_nested_file()`, `write_single_file()`
- `FuseMount` struct with MemoryStorage support (make it generic over `S: Storage`)
- `blocking()` helper

### New: `FailingStorage<S: Storage>` wrapper

In `tests/common/mod.rs`, wrapping `MemoryStorage` with `AtomicBool` flags for each method:
- `fail_put_blob`, `fail_put_node`, `fail_put_head`
- `fail_get_blob`, `fail_get_node`, `fail_get_head`

When a flag is set, returns `Err(MerkleError::Storage("injected failure"))`.

### Mirrored Tests via Generic Functions

Define test scenario functions that are generic over the storage backend:
- Library path: direct `Storage` trait calls + `MerkleFuse` with `MemoryStorage`
- FUSE path: `std::fs` calls through a mounted `MerkleFuse<MemoryStorage>`

Most FUSE tests can use `MemoryStorage` directly (no Docker needed) since `MerkleFuse<S>` is generic. Reserve Docker-backed tests only for S3-specific behaviors (hash verification, real CAS ETags).

## File Organization

```
tests/
  common/
    mod.rs                 -- shared helpers, FailingStorage, FuseMount generic
  integration_test.rs      -- keep existing + add tree/canonical edge cases
  rustfs_integ_test.rs     -- keep existing S3-backed tests (refactor to use common)
  fuse_integ_test.rs       -- refactor to use common, add MemoryStorage FUSE tests
  fuse_error_test.rs       -- NEW: FUSE error paths (errno checks) w/ MemoryStorage
  storage_error_test.rs    -- NEW: FailingStorage no-panic tests
  concurrency_test.rs      -- NEW: concurrent CAS, multi-branch writes
```

## Test Cases

### Phase 1: Extract Shared Helpers
1. Create `tests/common/mod.rs`
2. Move duplicated helpers from both test files
3. Make `FuseMount` generic: `FuseMount<S: Storage + Send + Sync + 'static>`
4. Verify existing tests still pass

### Phase 2: FailingStorage + No-Panic Tests (`tests/storage_error_test.rs`)

| Test | What it covers |
|------|----------------|
| `test_failing_put_blob_during_commit` | Toggle fail_put_blob, FUSE flush → EIO, no panic |
| `test_failing_put_node_during_commit` | Toggle fail_put_node, FUSE flush → EIO, no panic |
| `test_failing_put_head_during_commit` | Toggle fail_put_head, FUSE flush → EIO, no panic |
| `test_failing_get_blob_during_load` | fail_get_blob → MerkleFuse::new loads empty file content, no panic |
| `test_failing_get_node_during_load` | fail_get_node → load_tree_from_storage silently continues, no panic |
| `test_failing_get_head_during_load` | fail_get_head → MerkleFuse::new starts fresh, no panic |
| `test_failing_storage_destroy_no_panic` | All failures on, drop MerkleFuse → catch_unwind in destroy works |
| `test_intermittent_failure_then_recovery` | Fail first put, re-enable, retry → eventual success |

### Phase 3: FUSE Error Path Tests (`tests/fuse_error_test.rs`)

All use `MemoryStorage` (no Docker). Exercise every error branch in `src/fuse.rs`.

| Test | Expected errno | fuse.rs branch |
|------|----------------|----------------|
| `test_lookup_nonexistent_file` | ENOENT | lookup:559 |
| `test_getattr_invalid_ino` | ENOENT | getattr:566 |
| `test_readlink_on_regular_file` | EINVAL | readlink:626 |
| `test_mkdir_existing_name` | EEXIST | mkdir:643 |
| `test_mkdir_nonexistent_parent` | ENOENT | mkdir:647 (via bad path) |
| `test_unlink_nonexistent` | ENOENT | unlink:700 |
| `test_unlink_directory` | EISDIR | unlink:709 |
| `test_rmdir_nonempty` | ENOTEMPTY | rmdir:747 |
| `test_rmdir_on_file` | ENOTDIR | rmdir:751 |
| `test_rmdir_nonexistent` | ENOENT | rmdir:739 |
| `test_symlink_existing_name` | EEXIST | symlink:781 |
| `test_create_existing_name` | EEXIST | create:967 |
| `test_rename_nonexistent_source` | ENOENT | rename:839 |
| `test_read_past_eof` | returns 0 bytes | read:906 |
| `test_readdir_on_file` | ENOTDIR | readdir:1068 |
| `test_setattr_changes_mode_uid_gid_size_mtime` | verifies all setattr branches | setattr:596-616 |
| `test_open_o_trunc` | O_TRUNC clears data | open:878-886 |
| `test_write_to_nonexistent_ino` | ENOENT | write:932 |
| `test_rename_overwrite_existing` | replaces destination | rename:852-857 |
| `test_rename_across_dirs` | cross-directory rename | rename:860-870 |
| `test_empty_readdir_only_dots` | readdir on empty dir → only ".", ".." | readdir:1078-1085 |

### Phase 4: FUSE Happy Path Tests (with MemoryStorage)

Add to `fuse_integ_test.rs` (using MemoryStorage for speed):

| Test | Coverage |
|------|----------|
| `test_fuse_mem_write_read_empty_file` | 0-byte file |
| `test_fuse_mem_file_at_inline_threshold` | exactly 128 bytes → inline in commit |
| `test_fuse_mem_file_above_inline_threshold` | 129 bytes → blocks in commit |
| `test_fuse_mem_deeply_nested_10_levels` | a/b/c/d/e/f/g/h/i/j/file.txt |
| `test_fuse_mem_special_chars_in_filename` | spaces, dots, unicode, hyphens |
| `test_fuse_mem_very_long_filename` | 255-char filename |
| `test_fuse_mem_executable_permission` | create file with exec, verify persists |
| `test_fuse_mem_persistence_across_remount` | write, unmount, remount, read |
| `test_fuse_mem_partial_read_at_offset` | read with offset in middle |
| `test_fuse_mem_write_extending_file` | write at offset > length extends with zeros |
| `test_fuse_mem_large_file_1mb` | 1 MiB file through FUSE |

### Phase 5: Tree & Canonical Edge Cases (add to `tests/integration_test.rs`)

**Tree Operations:**

| Test | Coverage |
|------|----------|
| `test_insert_entry_into_interior_node_errors` | insert_entry on Interior → InvalidNode |
| `test_remove_entry_from_interior_node_errors` | remove_entry on Interior → InvalidNode |
| `test_insert_causing_split` | many entries > SPLIT_THRESHOLD → SplitResult |
| `test_needs_merge_small_node` | below MERGE_THRESHOLD → true |
| `test_needs_merge_large_node` | above MERGE_THRESHOLD → false |
| `test_entries_on_interior_returns_none` | entries() on interior → None |
| `test_entries_on_leaf_returns_entries` | entries() on leaf → Some |
| `test_lookup_exact_key_match_interior` | name == key → routes to idx+1 |

**Canonical Deserialization Errors:**

| Test | Coverage |
|------|----------|
| `test_deserialize_unknown_version` | v:999 → UnknownVersion |
| `test_deserialize_unknown_kind` | kind:"invalid" → InvalidNode |
| `test_deserialize_missing_name` | entry without name → InvalidNode |
| `test_deserialize_missing_type` | entry without type → InvalidNode |
| `test_deserialize_missing_v` | no v field → InvalidNode |
| `test_deserialize_missing_kind` | no kind field → InvalidNode |
| `test_deserialize_invalid_entry_type` | type:"bogus" → InvalidNode |
| `test_deserialize_invalid_child_hash_length` | child hash not 32 bytes → InvalidNode |
| `test_deserialize_non_string_map_key` | integer key → InvalidNode |
| `test_verify_canonical_tampered_bytes` | modify byte → CanonicalMismatch |
| `test_deserialize_missing_entries_array` | leaf without entries → InvalidNode |
| `test_deserialize_missing_keys_array` | interior without keys → InvalidNode |
| `test_deserialize_missing_children_array` | interior without children → InvalidNode |
| `test_deserialize_invalid_block_hash_length` | block hash not 32 bytes → InvalidNode |
| `test_deserialize_entry_not_a_map` | entry is a string → InvalidNode |
| `test_deserialize_node_not_a_map` | top level is an array → InvalidNode |
| `test_deserialize_invalid_inline_content` | inline is integer → InvalidNode |

**Hash Edge Cases:**

| Test | Coverage |
|------|----------|
| `test_hex_decode_invalid_hex` | non-hex chars → error |
| `test_hex_decode_wrong_length` | 16 bytes → error |

**CAS branch coverage (MemoryStorage put_head):**

| Test | Coverage |
|------|----------|
| `test_cas_none_etag_head_exists` | (Some, None) → CasConflict |
| `test_cas_some_etag_head_missing` | (None, Some) → CasConflict |

### Phase 6: Concurrency Tests (`tests/concurrency_test.rs`)

| Test | Coverage |
|------|----------|
| `test_concurrent_cas_race` | Two tasks race put_head; one CasConflict, retries succeed |
| `test_concurrent_writes_different_branches` | Parallel writes to separate branches both succeed |
| `test_concurrent_reads_while_writing` | Reader sees consistent snapshots during writes |
| `test_many_concurrent_blob_puts` | 50+ concurrent put_blob calls → no data corruption |
| `test_fuse_mem_concurrent_client` | FUSE writes, library client reads after flush → consistent |

### Phase 7: Coverage Analysis

1. Install: `cargo install cargo-llvm-cov`
2. Run: `cargo llvm-cov --branch --html`
3. Inspect uncovered branches in `src/fuse.rs`, `src/storage.rs`, `src/canonical.rs`, `src/tree.rs`
4. Add targeted tests for any remaining gaps
5. Iterate until full line + branch coverage

**Key coverage targets:**
- `fuse.rs`: every match arm in all 17 Filesystem trait methods + do_commit + destroy
- `storage.rs`: all 4 CAS branches in MemoryStorage::put_head, all error conversions
- `canonical.rs`: every error branch in value_to_tree_node and value_to_leaf_entry
- `tree.rs`: split path, merge threshold, interior node error paths
- `error.rs`: all error variants constructed somewhere
- `commit.rs`: with/without parent, hash determinism
- `hash.rs`: invalid hex decode paths
