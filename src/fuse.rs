use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};

use crate::commit::Commit;
use crate::error::MerkleError;
use crate::hash;
use crate::storage::Storage;
use crate::tree::*;

const TTL: Duration = Duration::from_secs(1);
const ROOT_INO: u64 = 1;
const BLOCK_SIZE: u32 = 4096;

// === Internal types ===

struct Inode {
    parent: u64,
    name: String,
    kind: InodeKind,
    uid: u32,
    gid: u32,
    mtime: SystemTime,
    ctime: SystemTime,
    exec: bool,
    size: u64,
    nlink: u32,
}

enum InodeKind {
    Directory { children: BTreeMap<String, u64> },
    File { data: Vec<u8> },
    Symlink { target: String },
}

impl Inode {
    fn file_type(&self) -> FileType {
        match &self.kind {
            InodeKind::Directory { .. } => FileType::Directory,
            InodeKind::File { .. } => FileType::RegularFile,
            InodeKind::Symlink { .. } => FileType::Symlink,
        }
    }

    fn file_attr(&self, ino: u64) -> FileAttr {
        let (kind, size, perm) = match &self.kind {
            InodeKind::Directory { .. } => (FileType::Directory, 0u64, 0o755u16),
            InodeKind::File { data } => {
                let perm = if self.exec { 0o755 } else { 0o644 };
                (FileType::RegularFile, data.len() as u64, perm)
            }
            InodeKind::Symlink { target } => (FileType::Symlink, target.len() as u64, 0o777),
        };

        FileAttr {
            ino,
            size,
            blocks: size.div_ceil(BLOCK_SIZE as u64),
            atime: self.mtime,
            mtime: self.mtime,
            ctime: self.ctime,
            crtime: self.ctime,
            kind,
            perm,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }
}

// === Pending entry types for committing ===

struct PendingEntry {
    name: String,
    kind: PendingKind,
}

enum PendingKind {
    File {
        data: Vec<u8>,
        exec: bool,
        mtime_ns: u64,
        ctime_ns: u64,
        uid: u64,
        gid: u64,
    },
    Dir {
        child_ino: u64,
    },
    Symlink {
        target: String,
        mtime_ns: u64,
        ctime_ns: u64,
        uid: u64,
        gid: u64,
    },
}

// === Public API ===

pub struct MerkleFuse<S> {
    storage: S,
    rt: tokio::runtime::Handle,
    branch: String,
    client_id: String,
    inodes: BTreeMap<u64, Inode>,
    next_ino: u64,
    dirty: bool,
    etag: Option<String>,
    parent_commit_hash: Option<[u8; 32]>,
}

impl<S: Storage + Send + Sync + 'static> MerkleFuse<S> {
    /// Create a new FUSE filesystem backed by the given storage.
    /// Loads the current tree from HEAD if it exists.
    pub async fn new(
        storage: S,
        rt: tokio::runtime::Handle,
        branch: String,
        client_id: String,
    ) -> Self {
        let now = SystemTime::now();
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

        let mut inodes = BTreeMap::new();
        inodes.insert(
            ROOT_INO,
            Inode {
                parent: ROOT_INO,
                name: String::new(),
                kind: InodeKind::Directory {
                    children: BTreeMap::new(),
                },
                uid,
                gid,
                mtime: now,
                ctime: now,
                exec: false,
                size: 0,
                nlink: 2,
            },
        );

        let mut next_ino = 2u64;
        let mut etag = None;
        let mut parent_commit_hash = None;

        if let Ok(Some((commit, e))) = storage.get_head(&branch).await {
            etag = Some(e);
            parent_commit_hash = Some(commit.hash().unwrap());
            load_tree_from_storage(&storage, &commit.root, ROOT_INO, &mut inodes, &mut next_ino)
                .await;
        }

        Self {
            storage,
            rt,
            branch,
            client_id,
            inodes,
            next_ino,
            dirty: false,
            etag,
            parent_commit_hash,
        }
    }

    /// Mount the filesystem at the given path. Returns a session handle;
    /// dropping it unmounts the filesystem.
    pub fn mount(self, mountpoint: &Path) -> std::io::Result<fuser::BackgroundSession> {
        let options = vec![
            MountOption::RW,
            MountOption::FSName("merkle-drive".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowOther,
        ];
        fuser::spawn_mount2(self, mountpoint, &options)
    }

    /// Commit the current in-memory state to storage.
    async fn do_commit(&mut self) -> Result<(), MerkleError> {
        if !self.dirty {
            return Ok(());
        }

        // Collect directory tree in bottom-up order
        let dirs = collect_dirs_bottom_up(&self.inodes, ROOT_INO);

        // Upload directories bottom-up
        let mut dir_hashes: std::collections::HashMap<u64, String> =
            std::collections::HashMap::new();

        for (dir_ino, entries) in &dirs {
            let mut leaf_entries = Vec::new();

            for pe in entries {
                let entry = match &pe.kind {
                    PendingKind::File {
                        data,
                        exec,
                        mtime_ns,
                        ctime_ns,
                        uid,
                        gid,
                    } => {
                        let content = if data.len() <= DEFAULT_INLINE_THRESHOLD as usize {
                            FileContent::Inline(data.clone())
                        } else {
                            let hex = self.storage.put_blob(data).await?;
                            let h = hash::hex_decode(&hex)?;
                            FileContent::Blocks(vec![h])
                        };
                        LeafEntry::file(
                            pe.name.clone(),
                            data.len() as u64,
                            content,
                            *mtime_ns,
                            *ctime_ns,
                            *uid,
                            *gid,
                            *exec,
                        )
                    }
                    PendingKind::Dir { child_ino } => {
                        let hash_hex = dir_hashes.get(child_ino).ok_or_else(|| {
                            MerkleError::NotFound(format!("dir hash for ino {child_ino}"))
                        })?;
                        let h = hash::hex_decode(hash_hex)?;
                        LeafEntry::dir(pe.name.clone(), h)
                    }
                    PendingKind::Symlink {
                        target,
                        mtime_ns,
                        ctime_ns,
                        uid,
                        gid,
                    } => LeafEntry::symlink(
                        pe.name.clone(),
                        target.clone(),
                        *mtime_ns,
                        *ctime_ns,
                        *uid,
                        *gid,
                    ),
                };
                leaf_entries.push(entry);
            }

            let node = TreeNode::Leaf(LeafNode {
                entries: leaf_entries,
            });
            let node_hash = self.storage.put_node(&node).await?;
            dir_hashes.insert(*dir_ino, node_hash);
        }

        // Create commit
        let root_hex = dir_hashes
            .get(&ROOT_INO)
            .ok_or_else(|| MerkleError::NotFound("root hash".into()))?;
        let root_hash = hash::hex_decode(root_hex)?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let commit = Commit::new(
            root_hash,
            self.parent_commit_hash,
            self.client_id.clone(),
            now_ms,
            "fuse commit".to_string(),
        );

        let new_etag = self
            .storage
            .put_head(&self.branch, &commit, self.etag.as_deref())
            .await?;

        let commit_hash = commit.hash()?;
        self.dirty = false;
        self.etag = Some(new_etag);
        self.parent_commit_hash = Some(commit_hash);

        Ok(())
    }
}

// === Tree loading helpers ===

/// Load a directory tree from storage into the inode table (iterative BFS).
async fn load_tree_from_storage<S: Storage>(
    storage: &S,
    root_hash: &str,
    root_ino: u64,
    inodes: &mut BTreeMap<u64, Inode>,
    next_ino: &mut u64,
) {
    let mut queue = vec![(root_ino, root_hash.to_string())];

    while let Some((dir_ino, node_hash)) = queue.pop() {
        let entries = match collect_all_leaf_entries(storage, &node_hash).await {
            Ok(e) => e,
            Err(_) => continue,
        };

        for entry in entries {
            let ino = *next_ino;
            *next_ino += 1;

            let now = SystemTime::now();
            let mtime = entry
                .mtime
                .map(|ns| UNIX_EPOCH + Duration::from_nanos(ns))
                .unwrap_or(now);
            let ctime = entry
                .ctime
                .map(|ns| UNIX_EPOCH + Duration::from_nanos(ns))
                .unwrap_or(now);
            let uid = entry.uid.unwrap_or(0) as u32;
            let gid = entry.gid.unwrap_or(0) as u32;

            match entry.entry_type {
                EntryType::File => {
                    let data = load_file_content(storage, &entry).await;
                    let size = data.len() as u64;
                    inodes.insert(
                        ino,
                        Inode {
                            parent: dir_ino,
                            name: entry.name.clone(),
                            kind: InodeKind::File { data },
                            uid,
                            gid,
                            mtime,
                            ctime,
                            exec: entry.exec,
                            size,
                            nlink: 1,
                        },
                    );
                    add_child_to_parent(inodes, dir_ino, &entry.name, ino);
                }
                EntryType::Dir => {
                    inodes.insert(
                        ino,
                        Inode {
                            parent: dir_ino,
                            name: entry.name.clone(),
                            kind: InodeKind::Directory {
                                children: BTreeMap::new(),
                            },
                            uid,
                            gid,
                            mtime,
                            ctime,
                            exec: false,
                            size: 0,
                            nlink: 2,
                        },
                    );
                    add_child_to_parent(inodes, dir_ino, &entry.name, ino);
                    if let Some(parent) = inodes.get_mut(&dir_ino) {
                        parent.nlink += 1;
                    }
                    if let Some(h) = &entry.hash {
                        queue.push((ino, hash::hex_encode(h)));
                    }
                }
                EntryType::Symlink => {
                    inodes.insert(
                        ino,
                        Inode {
                            parent: dir_ino,
                            name: entry.name.clone(),
                            kind: InodeKind::Symlink {
                                target: entry.target.unwrap_or_default(),
                            },
                            uid,
                            gid,
                            mtime,
                            ctime,
                            exec: false,
                            size: 0,
                            nlink: 1,
                        },
                    );
                    add_child_to_parent(inodes, dir_ino, &entry.name, ino);
                }
            }
        }
    }
}

fn add_child_to_parent(
    inodes: &mut BTreeMap<u64, Inode>,
    parent_ino: u64,
    name: &str,
    child_ino: u64,
) {
    if let Some(parent) = inodes.get_mut(&parent_ino) {
        if let InodeKind::Directory { children } = &mut parent.kind {
            children.insert(name.to_string(), child_ino);
        }
    }
}

/// Flatten a B-tree node into all leaf entries (handles interior nodes iteratively).
async fn collect_all_leaf_entries<S: Storage>(
    storage: &S,
    node_hash: &str,
) -> crate::error::Result<Vec<LeafEntry>> {
    let mut entries = Vec::new();
    let mut stack = vec![node_hash.to_string()];

    while let Some(h) = stack.pop() {
        let node = storage.get_node(&h).await?;
        match node {
            TreeNode::Leaf(leaf) => entries.extend(leaf.entries),
            TreeNode::Interior(interior) => {
                for child_hash in interior.children.iter().rev() {
                    stack.push(hash::hex_encode(child_hash));
                }
            }
        }
    }
    Ok(entries)
}

async fn load_file_content<S: Storage>(storage: &S, entry: &LeafEntry) -> Vec<u8> {
    match &entry.content {
        Some(FileContent::Inline(data)) => data.clone(),
        Some(FileContent::Blocks(blocks)) => {
            let mut result = Vec::new();
            for block in blocks {
                let hex = hash::hex_encode(block);
                if let Ok(data) = storage.get_blob(&hex).await {
                    result.extend_from_slice(&data);
                }
            }
            result
        }
        None => Vec::new(),
    }
}

// === Commit helpers ===

/// Collect all directories in bottom-up (post-order) for uploading.
fn collect_dirs_bottom_up(
    inodes: &BTreeMap<u64, Inode>,
    root_ino: u64,
) -> Vec<(u64, Vec<PendingEntry>)> {
    let mut result = Vec::new();
    collect_dirs_recursive(inodes, root_ino, &mut result);
    result
}

fn collect_dirs_recursive(
    inodes: &BTreeMap<u64, Inode>,
    dir_ino: u64,
    result: &mut Vec<(u64, Vec<PendingEntry>)>,
) {
    let children = match inodes.get(&dir_ino).map(|i| &i.kind) {
        Some(InodeKind::Directory { children }) => children,
        _ => return,
    };

    // Recurse into child directories first (bottom-up)
    for &child_ino in children.values() {
        if matches!(
            inodes.get(&child_ino).map(|i| &i.kind),
            Some(InodeKind::Directory { .. })
        ) {
            collect_dirs_recursive(inodes, child_ino, result);
        }
    }

    // Collect this directory's entries
    let entries: Vec<PendingEntry> = children
        .iter()
        .map(|(name, &child_ino)| {
            let child = &inodes[&child_ino];
            let mtime_ns = child
                .mtime
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let ctime_ns = child
                .ctime
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;

            let kind = match &child.kind {
                InodeKind::File { data } => PendingKind::File {
                    data: data.clone(),
                    exec: child.exec,
                    mtime_ns,
                    ctime_ns,
                    uid: child.uid as u64,
                    gid: child.gid as u64,
                },
                InodeKind::Directory { .. } => PendingKind::Dir { child_ino },
                InodeKind::Symlink { target } => PendingKind::Symlink {
                    target: target.clone(),
                    mtime_ns,
                    ctime_ns,
                    uid: child.uid as u64,
                    gid: child.gid as u64,
                },
            };

            PendingEntry {
                name: name.clone(),
                kind,
            }
        })
        .collect();

    result.push((dir_ino, entries));
}

// === Filesystem trait implementation ===

impl<S: Storage + Send + Sync + 'static> Filesystem for MerkleFuse<S> {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let children = match self.inodes.get(&parent).map(|i| &i.kind) {
            Some(InodeKind::Directory { children }) => children,
            _ => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        match children.get(name_str) {
            Some(&ino) => {
                let attr = self.inodes[&ino].file_attr(ino);
                reply.entry(&TTL, &attr, 0);
            }
            None => reply.error(libc::ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self.inodes.get(&ino) {
            Some(inode) => reply.attr(&TTL, &inode.file_attr(ino)),
            None => reply.error(libc::ENOENT),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let inode = match self.inodes.get_mut(&ino) {
            Some(i) => i,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if let Some(uid_val) = uid {
            inode.uid = uid_val;
        }
        if let Some(gid_val) = gid {
            inode.gid = gid_val;
        }
        if let Some(mode_val) = mode {
            inode.exec = mode_val & 0o111 != 0;
        }
        if let Some(size_val) = size {
            if let InodeKind::File { data } = &mut inode.kind {
                data.resize(size_val as usize, 0);
                inode.size = size_val;
            }
        }
        if let Some(mtime_val) = mtime {
            inode.mtime = match mtime_val {
                TimeOrNow::SpecificTime(t) => t,
                TimeOrNow::Now => SystemTime::now(),
            };
        }

        self.dirty = true;
        let attr = self.inodes[&ino].file_attr(ino);
        reply.attr(&TTL, &attr);
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.inodes.get(&ino).map(|i| &i.kind) {
            Some(InodeKind::Symlink { target }) => reply.data(target.as_bytes()),
            _ => reply.error(libc::EINVAL),
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_str = name.to_str().unwrap_or("").to_string();

        if let Some(InodeKind::Directory { children }) = self.inodes.get(&parent).map(|i| &i.kind) {
            if children.contains_key(&name_str) {
                reply.error(libc::EEXIST);
                return;
            }
        } else {
            reply.error(libc::ENOENT);
            return;
        }

        let ino = self.next_ino;
        self.next_ino += 1;
        let now = SystemTime::now();

        self.inodes.insert(
            ino,
            Inode {
                parent,
                name: name_str.clone(),
                kind: InodeKind::Directory {
                    children: BTreeMap::new(),
                },
                uid: req.uid(),
                gid: req.gid(),
                mtime: now,
                ctime: now,
                exec: false,
                size: 0,
                nlink: 2,
            },
        );

        if let Some(parent_inode) = self.inodes.get_mut(&parent) {
            if let InodeKind::Directory { children } = &mut parent_inode.kind {
                children.insert(name_str, ino);
            }
            parent_inode.nlink += 1;
        }

        self.dirty = true;
        let attr = self.inodes[&ino].file_attr(ino);
        reply.entry(&TTL, &attr, 0);
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let child_ino = match self.inodes.get(&parent).and_then(|i| match &i.kind {
            InodeKind::Directory { children } => children.get(name_str).copied(),
            _ => None,
        }) {
            Some(ino) => ino,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if matches!(
            self.inodes.get(&child_ino).map(|i| &i.kind),
            Some(InodeKind::Directory { .. })
        ) {
            reply.error(libc::EISDIR);
            return;
        }

        self.inodes.remove(&child_ino);
        if let Some(parent_inode) = self.inodes.get_mut(&parent) {
            if let InodeKind::Directory { children } = &mut parent_inode.kind {
                children.remove(name_str);
            }
        }

        self.dirty = true;
        reply.ok();
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let child_ino = match self.inodes.get(&parent).and_then(|i| match &i.kind {
            InodeKind::Directory { children } => children.get(name_str).copied(),
            _ => None,
        }) {
            Some(ino) => ino,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        match self.inodes.get(&child_ino).map(|i| &i.kind) {
            Some(InodeKind::Directory { children }) if children.is_empty() => {}
            Some(InodeKind::Directory { .. }) => {
                reply.error(libc::ENOTEMPTY);
                return;
            }
            _ => {
                reply.error(libc::ENOTDIR);
                return;
            }
        }

        self.inodes.remove(&child_ino);
        if let Some(parent_inode) = self.inodes.get_mut(&parent) {
            if let InodeKind::Directory { children } = &mut parent_inode.kind {
                children.remove(name_str);
            }
            parent_inode.nlink = parent_inode.nlink.saturating_sub(1);
        }

        self.dirty = true;
        reply.ok();
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let name_str = link_name.to_str().unwrap_or("").to_string();
        let target_str = target.to_str().unwrap_or("").to_string();

        if let Some(InodeKind::Directory { children }) = self.inodes.get(&parent).map(|i| &i.kind) {
            if children.contains_key(&name_str) {
                reply.error(libc::EEXIST);
                return;
            }
        } else {
            reply.error(libc::ENOENT);
            return;
        }

        let ino = self.next_ino;
        self.next_ino += 1;
        let now = SystemTime::now();

        self.inodes.insert(
            ino,
            Inode {
                parent,
                name: name_str.clone(),
                kind: InodeKind::Symlink { target: target_str },
                uid: req.uid(),
                gid: req.gid(),
                mtime: now,
                ctime: now,
                exec: false,
                size: 0,
                nlink: 1,
            },
        );

        if let Some(parent_inode) = self.inodes.get_mut(&parent) {
            if let InodeKind::Directory { children } = &mut parent_inode.kind {
                children.insert(name_str, ino);
            }
        }

        self.dirty = true;
        let attr = self.inodes[&ino].file_attr(ino);
        reply.entry(&TTL, &attr, 0);
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let name_str = name.to_str().unwrap_or("");
        let newname_str = newname.to_str().unwrap_or("").to_string();

        let child_ino = match self.inodes.get(&parent).and_then(|i| match &i.kind {
            InodeKind::Directory { children } => children.get(name_str).copied(),
            _ => None,
        }) {
            Some(ino) => ino,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Remove from old parent
        if let Some(p) = self.inodes.get_mut(&parent) {
            if let InodeKind::Directory { children } = &mut p.kind {
                children.remove(name_str);
            }
        }

        // Remove any existing entry at destination
        if let Some(old_ino) = self.inodes.get(&newparent).and_then(|i| match &i.kind {
            InodeKind::Directory { children } => children.get(&newname_str).copied(),
            _ => None,
        }) {
            self.inodes.remove(&old_ino);
        }

        // Add to new parent
        if let Some(p) = self.inodes.get_mut(&newparent) {
            if let InodeKind::Directory { children } = &mut p.kind {
                children.insert(newname_str.clone(), child_ino);
            }
        }

        // Update inode metadata
        if let Some(inode) = self.inodes.get_mut(&child_ino) {
            inode.name = newname_str;
            inode.parent = newparent;
        }

        self.dirty = true;
        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        // Handle O_TRUNC
        if flags & libc::O_TRUNC != 0 {
            if let Some(inode) = self.inodes.get_mut(&ino) {
                if let InodeKind::File { data } = &mut inode.kind {
                    data.clear();
                    inode.size = 0;
                    inode.mtime = SystemTime::now();
                    self.dirty = true;
                }
            }
        }
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.inodes.get(&ino).map(|i| &i.kind) {
            Some(InodeKind::File { data }) => {
                let start = offset as usize;
                if start >= data.len() {
                    reply.data(&[]);
                } else {
                    let end = (start + size as usize).min(data.len());
                    reply.data(&data[start..end]);
                }
            }
            _ => reply.error(libc::ENOENT),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let inode = match self.inodes.get_mut(&ino) {
            Some(i) => i,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        match &mut inode.kind {
            InodeKind::File { data: file_data } => {
                let offset = offset as usize;
                let end = offset + data.len();
                if end > file_data.len() {
                    file_data.resize(end, 0);
                }
                file_data[offset..end].copy_from_slice(data);
                inode.size = file_data.len() as u64;
                inode.mtime = SystemTime::now();
                self.dirty = true;
                reply.written(data.len() as u32);
            }
            _ => reply.error(libc::EISDIR),
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name_str = name.to_str().unwrap_or("").to_string();

        if let Some(InodeKind::Directory { children }) = self.inodes.get(&parent).map(|i| &i.kind) {
            if children.contains_key(&name_str) {
                reply.error(libc::EEXIST);
                return;
            }
        } else {
            reply.error(libc::ENOENT);
            return;
        }

        let ino = self.next_ino;
        self.next_ino += 1;
        let now = SystemTime::now();
        let exec = mode & 0o111 != 0;

        self.inodes.insert(
            ino,
            Inode {
                parent,
                name: name_str.clone(),
                kind: InodeKind::File { data: Vec::new() },
                uid: req.uid(),
                gid: req.gid(),
                mtime: now,
                ctime: now,
                exec,
                size: 0,
                nlink: 1,
            },
        );

        if let Some(parent_inode) = self.inodes.get_mut(&parent) {
            if let InodeKind::Directory { children } = &mut parent_inode.kind {
                children.insert(name_str, ino);
            }
        }

        self.dirty = true;
        let attr = self.inodes[&ino].file_attr(ino);
        reply.created(&TTL, &attr, 0, 0, 0);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        let rt = self.rt.clone();
        match rt.block_on(self.do_commit()) {
            Ok(()) => reply.ok(),
            Err(e) => {
                eprintln!("merkle-drive fuse: flush commit failed: {e}");
                reply.error(libc::EIO);
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let rt = self.rt.clone();
        match rt.block_on(self.do_commit()) {
            Ok(()) => reply.ok(),
            Err(e) => {
                eprintln!("merkle-drive fuse: fsync commit failed: {e}");
                reply.error(libc::EIO);
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let (parent_ino, children) = match self.inodes.get(&ino) {
            Some(inode) => match &inode.kind {
                InodeKind::Directory { children } => (inode.parent, children),
                _ => {
                    reply.error(libc::ENOTDIR);
                    return;
                }
            },
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let mut entries: Vec<(u64, FileType, String)> = vec![
            (ino, FileType::Directory, ".".to_string()),
            (parent_ino, FileType::Directory, "..".to_string()),
        ];
        for (name, &child_ino) in children {
            let ft = self.inodes[&child_ino].file_type();
            entries.push((child_ino, ft, name.clone()));
        }

        for (i, (entry_ino, ft, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*entry_ino, (i + 1) as i64, *ft, name) {
                break;
            }
        }
        reply.ok();
    }

    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn destroy(&mut self) {
        let rt = self.rt.clone();
        // Catch panics from tokio runtime shutdown races
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = rt.block_on(self.do_commit());
        }));
    }
}
