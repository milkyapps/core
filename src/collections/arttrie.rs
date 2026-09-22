//! Adaptive Radix Tree (ART)

use crate::pagemgr::{AllocId, PageManager, TypedAlloc};
use std::path::Path;

/// Maximum compressed path prefix stored inline inside an inner node.
const MAX_PREFIX: usize = 64;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Tag {
    Node4 = 1,
    Node16 = 2,
    Node48 = 3,
    Node256 = 4,
    Leaf = 5,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NodeHeader {
    tag: u8,
    count: u8,
    prefix_len: u8,
    _pad: u8,
    prefix: [u8; MAX_PREFIX],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Node4 {
    header: NodeHeader,
    keys: [u8; 4],
    /// Empty slots are [`AllocId::invalid`] (not `Option` — avoids undef bytes in dumps).
    children: [AllocId; 4],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Node16 {
    header: NodeHeader,
    keys: [u8; 16],
    /// Keeps `children` 8-byte aligned; must stay zero for deterministic dumps.
    _pad: [u8; 4],
    children: [AllocId; 16],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Node48 {
    header: NodeHeader,
    /// `0` = empty; otherwise `idx` means `children[idx - 1]`.
    keys: [u8; 256],
    _pad: [u8; 4],
    children: [AllocId; 48],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Node256 {
    header: NodeHeader,
    _pad: [u8; 4],
    children: [AllocId; 256],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Leaf {
    tag: u8,
    _pad: [u8; 3],
    key_len: u32,
    value_len: u32,
    /// Padding so `key_blob` is 8-byte aligned.
    _pad2: [u8; 4],
    key_blob: AllocId,
    value_blob: AllocId,
}

/// Outcome of [`ArtTrie::get`], including search statistics.
#[derive(Debug)]
pub struct GetResult<'a> {
    /// Value bytes when the key was found.
    pub value: Option<&'a [u8]>,
    /// How the search traversed the trie.
    pub stats: GetStats,
}

impl GetResult<'_> {
    /// `true` when [`Self::value`] is `Some`.
    #[must_use]
    pub const fn found(&self) -> bool {
        self.value.is_some()
    }
}

/// Counters collected while walking the trie during a get.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GetStats {
    /// Inner + leaf nodes examined.
    pub nodes_visited: usize,
    /// Inner nodes only (Node4/16/48/256).
    pub inner_nodes_visited: usize,
    /// Successful child-pointer follows (`depth += 1` hops).
    pub edges_followed: usize,
    /// Key bytes consumed by matching compressed prefixes (path compression win).
    pub prefix_bytes_matched: usize,
    /// Whether a leaf was reached and its full key compared.
    pub leaf_compared: bool,
}

impl GetStats {
    /// Compact multi-line dump for snapshot transcripts.
    #[must_use]
    pub fn debug_dump(self) -> String {
        format!(
            "stats: nodes={} inner={} edges={} prefix_bytes={} leaf_compared={}\n",
            self.nodes_visited,
            self.inner_nodes_visited,
            self.edges_followed,
            self.prefix_bytes_matched,
            self.leaf_compared
        )
    }
}

/// Persistent ART map from byte keys to byte values.
pub struct ArtTrie {
    pages: PageManager,
    root: Option<AllocId>,
}

impl ArtTrie {
    /// Opens (or creates) an ART store at `path` with default 64 KiB pages.
    #[must_use]
    pub fn open(path: impl AsRef<Path>) -> Option<Self> {
        Self::open_with_options(path, 65536, 16).ok()
    }

    /// Opens (or creates) an ART store with an explicit logical page size.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] from [`PageManager::open_with_options`].
    pub fn open_with_options(
        path: impl AsRef<Path>,
        page_size: usize,
        dirties_before_flush: usize,
    ) -> std::io::Result<Self> {
        let pages = PageManager::open_with_options(path, page_size, dirties_before_flush)?;
        Ok(Self { pages, root: None })
    }

    /// Human-readable dump of the logical trie, root handle, and page manager.
    #[must_use]
    pub fn debug_dump(&self) -> String {
        self.debug_dump_with_pages(true)
    }

    /// Like [`Self::debug_dump`], but page hex dumps can be omitted for large trees.
    #[must_use]
    pub fn debug_dump_with_pages(&self, include_pages: bool) -> String {
        let mut out = String::new();
        out.push_str("trie:\n");
        match self.root {
            None => out.push_str("  (empty)\n"),
            Some(id) => self.dump_node(&mut out, id, 1),
        }
        out.push('\n');
        match self.root {
            None => out.push_str("root: None\n"),
            Some(id) => out.push_str(&format!(
                "root: page={} slot={} size={}\n",
                id.page_id().index(),
                id.slot_id().index(),
                id.size()
            )),
        }
        if include_pages {
            out.push_str(&self.pages.debug_dump());
        }
        out
    }

    fn dump_node(&self, out: &mut String, id: AllocId, indent: usize) {
        let pad = "  ".repeat(indent);
        let loc = format!("[p{}s{}]", id.page_id().index(), id.slot_id().index());
        let tag = unsafe { *self.pages.slot_ptr::<u8>(id) };

        match tag {
            t if t == Tag::Leaf as u8 => {
                let leaf = unsafe { *self.pages.slot_ptr::<Leaf>(id) };
                let key = self.blob_preview(leaf.key_blob, leaf.key_len as usize);
                let value = self.blob_preview(leaf.value_blob, leaf.value_len as usize);
                out.push_str(&format!("{pad}Leaf {loc} key={key:?} value={value:?}\n"));
            }
            t if t == Tag::Node4 as u8 => {
                let n = unsafe { *self.pages.slot_ptr::<Node4>(id) };
                self.dump_inner(out, indent, "Node4", &loc, &n.header);
                for i in 0..usize::from(n.header.count) {
                    if !n.children[i].is_valid() {
                        continue;
                    }
                    out.push_str(&format!(
                        "{pad}  '{}'/0x{:02x} ->\n",
                        escape_byte(n.keys[i]),
                        n.keys[i]
                    ));
                    self.dump_node(out, n.children[i], indent + 2);
                }
            }
            t if t == Tag::Node16 as u8 => {
                let n = unsafe { *self.pages.slot_ptr::<Node16>(id) };
                self.dump_inner(out, indent, "Node16", &loc, &n.header);
                for i in 0..usize::from(n.header.count) {
                    if !n.children[i].is_valid() {
                        continue;
                    }
                    out.push_str(&format!(
                        "{pad}  '{}'/0x{:02x} ->\n",
                        escape_byte(n.keys[i]),
                        n.keys[i]
                    ));
                    self.dump_node(out, n.children[i], indent + 2);
                }
            }
            t if t == Tag::Node48 as u8 => {
                let n = unsafe { *self.pages.slot_ptr::<Node48>(id) };
                self.dump_inner(out, indent, "Node48", &loc, &n.header);
                for b in 0..=255u8 {
                    let idx = n.keys[usize::from(b)];
                    if idx == 0 {
                        continue;
                    }
                    let child = n.children[usize::from(idx) - 1];
                    if !child.is_valid() {
                        continue;
                    }
                    out.push_str(&format!("{pad}  '{}'/0x{:02x} ->\n", escape_byte(b), b));
                    self.dump_node(out, child, indent + 2);
                }
            }
            t if t == Tag::Node256 as u8 => {
                let n = unsafe { *self.pages.slot_ptr::<Node256>(id) };
                self.dump_inner(out, indent, "Node256", &loc, &n.header);
                for b in 0..=255u8 {
                    let child = n.children[usize::from(b)];
                    if !child.is_valid() {
                        continue;
                    }
                    out.push_str(&format!("{pad}  '{}'/0x{:02x} ->\n", escape_byte(b), b));
                    self.dump_node(out, child, indent + 2);
                }
            }
            other => {
                out.push_str(&format!("{pad}Unknown(tag={other}) {loc}\n"));
            }
        }
    }

    fn dump_inner(
        &self,
        out: &mut String,
        indent: usize,
        kind: &str,
        loc: &str,
        header: &NodeHeader,
    ) {
        let pad = "  ".repeat(indent);
        let plen = usize::from(header.prefix_len);
        let prefix = &header.prefix[..plen];
        let prefix_s = String::from_utf8_lossy(prefix);
        out.push_str(&format!(
            "{pad}{kind} {loc} count={} prefix={:?}\n",
            header.count, prefix_s
        ));
    }

    fn blob_preview(&self, blob: AllocId, len: usize) -> String {
        let n = len.min(blob.size());
        let ptr = self.pages.slot_ptr::<u8>(blob);
        // SAFETY: `blob` is a live allocation; `n` fits in the slot.
        let bytes = unsafe { std::slice::from_raw_parts(ptr, n) };
        String::from_utf8_lossy(bytes).into_owned()
    }

    /// Inserts or replaces `key` → `value`.
    ///
    /// Empty keys are ignored.
    ///
    /// # Panics
    ///
    /// Panics if the page manager cannot allocate a node or blob slot.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        if key.is_empty() {
            return;
        }
        if self.root.is_none() {
            self.root = Some(self.alloc_leaf(key, value));
            return;
        }

        let mut node = self.root.unwrap();
        let mut depth = 0usize;
        let mut parent: Option<(AllocId, u8)> = None; // parent id + byte that links to `node`

        loop {
            let tag = self.tag(node);
            if tag == Tag::Leaf as u8 {
                self.insert_at_leaf(node, parent, depth, key, value);
                return;
            }

            let prefix_len = self.prefix_len(node);
            let mismatch = self.prefix_mismatch(node, key, depth);
            if mismatch < prefix_len {
                self.insert_split_prefix(node, parent, depth, mismatch, key, value);
                return;
            }
            depth += prefix_len;

            if depth >= key.len() {
                // Proper prefix of an existing key — not supported in this build.
                return;
            }

            let byte = key[depth];
            if let Some(child) = self.find_child(node, byte) {
                parent = Some((node, byte));
                node = child;
                depth += 1;
            } else {
                let leaf = self.alloc_leaf(key, value);
                self.add_child_to(node, byte, leaf);
                return;
            }
        }
    }

    /// Looks up `key`, returning the value (if any) and search statistics.
    pub fn get(&mut self, key: &[u8]) -> GetResult<'_> {
        let mut stats = GetStats::default();

        if key.is_empty() {
            return GetResult { value: None, stats };
        }
        let Some(mut node) = self.root else {
            return GetResult { value: None, stats };
        };
        let mut depth = 0usize;

        loop {
            stats.nodes_visited += 1;
            let tag = self.tag(node);
            if tag == Tag::Leaf as u8 {
                stats.leaf_compared = true;
                let (key_blob, key_len, value_blob, value_len) = {
                    let leaf = *self.pages.get_mut::<Leaf>(node);
                    (leaf.key_blob, leaf.key_len, leaf.value_blob, leaf.value_len)
                };
                if !self.blob_eq(key_blob, key_len as usize, key) {
                    return GetResult { value: None, stats };
                }
                let bytes = self.pages.slot_bytes_mut(value_blob);
                return GetResult {
                    value: Some(&bytes[..value_len as usize]),
                    stats,
                };
            }

            stats.inner_nodes_visited += 1;
            let prefix_len = self.prefix_len(node);
            let matched = self.prefix_mismatch(node, key, depth);
            if matched < prefix_len {
                return GetResult { value: None, stats };
            }
            stats.prefix_bytes_matched += matched;
            depth += prefix_len;
            if depth >= key.len() {
                return GetResult { value: None, stats };
            }
            let byte = key[depth];
            match self.find_child(node, byte) {
                Some(child) => {
                    stats.edges_followed += 1;
                    node = child;
                    depth += 1;
                }
                None => {
                    return GetResult { value: None, stats };
                }
            }
        }
    }
}

fn escape_byte(b: u8) -> char {
    if (0x20..0x7f).contains(&b) {
        char::from(b)
    } else {
        '·'
    }
}

impl ArtTrie {
    fn tag(&mut self, id: AllocId) -> u8 {
        self.pages.slot_bytes_mut(id)[0]
    }

    fn prefix_len(&mut self, id: AllocId) -> usize {
        usize::from(self.pages.slot_bytes_mut(id)[2])
    }

    fn prefix_mismatch(&mut self, id: AllocId, key: &[u8], depth: usize) -> usize {
        let header = {
            let bytes = self.pages.slot_bytes_mut(id);
            // NodeHeader: tag, count, prefix_len, pad, prefix[8]
            let prefix_len = usize::from(bytes[2]);
            let mut prefix = [0u8; MAX_PREFIX];
            prefix.copy_from_slice(&bytes[4..4 + MAX_PREFIX]);
            (prefix_len, prefix)
        };
        let (prefix_len, prefix) = header;
        let mut i = 0;
        while i < prefix_len && depth + i < key.len() {
            if key[depth + i] != prefix[i] {
                break;
            }
            i += 1;
        }
        i
    }

    fn blob_eq(&mut self, blob: AllocId, len: usize, key: &[u8]) -> bool {
        if len != key.len() {
            return false;
        }
        let bytes = self.pages.slot_bytes_mut(blob);
        bytes[..len] == *key
    }

    fn alloc_blob(&mut self, data: &[u8]) -> AllocId {
        let size = data.len().next_power_of_two().max(8);
        let id = self
            .pages
            .alloc_slot(size)
            .expect("page manager alloc_slot failed");
        self.pages.slot_bytes_mut(id)[..data.len()].copy_from_slice(data);
        id
    }

    fn alloc_leaf(&mut self, key: &[u8], value: &[u8]) -> AllocId {
        let key_blob = self.alloc_blob(key);
        let value_blob = self.alloc_blob(value);
        let leaf = TypedAlloc::<Leaf>::alloc(&mut self.pages).expect("alloc leaf");
        *leaf.get_mut(&mut self.pages) = Leaf {
            tag: Tag::Leaf as u8,
            _pad: [0; 3],
            key_len: u32::try_from(key.len()).expect("key too long"),
            value_len: u32::try_from(value.len()).expect("value too long"),
            _pad2: [0; 4],
            key_blob,
            value_blob,
        };
        leaf.id()
    }

    fn find_child(&mut self, node: AllocId, byte: u8) -> Option<AllocId> {
        match self.tag(node) {
            t if t == Tag::Node4 as u8 => {
                let n = *self.pages.get_mut::<Node4>(node);
                (0..usize::from(n.header.count)).find_map(|i| {
                    if n.keys[i] == byte && n.children[i].is_valid() {
                        Some(n.children[i])
                    } else {
                        None
                    }
                })
            }
            t if t == Tag::Node16 as u8 => {
                let n = *self.pages.get_mut::<Node16>(node);
                (0..usize::from(n.header.count)).find_map(|i| {
                    if n.keys[i] == byte && n.children[i].is_valid() {
                        Some(n.children[i])
                    } else {
                        None
                    }
                })
            }
            t if t == Tag::Node48 as u8 => {
                let n = *self.pages.get_mut::<Node48>(node);
                let idx = n.keys[usize::from(byte)];
                if idx == 0 {
                    None
                } else {
                    let c = n.children[usize::from(idx) - 1];
                    c.is_valid().then_some(c)
                }
            }
            t if t == Tag::Node256 as u8 => {
                let n = *self.pages.get_mut::<Node256>(node);
                let c = n.children[usize::from(byte)];
                c.is_valid().then_some(c)
            }
            _ => None,
        }
    }

    fn insert_at_leaf(
        &mut self,
        leaf_id: AllocId,
        parent: Option<(AllocId, u8)>,
        depth: usize,
        key: &[u8],
        value: &[u8],
    ) {
        let (old_key_blob, old_key_len, old_value_blob) = {
            let leaf = *self.pages.get_mut::<Leaf>(leaf_id);
            (leaf.key_blob, leaf.key_len as usize, leaf.value_blob)
        };

        if self.blob_eq(old_key_blob, old_key_len, key) {
            // Replace value.
            self.pages.dealloc(old_value_blob);
            let value_blob = self.alloc_blob(value);
            let leaf = self.pages.get_mut::<Leaf>(leaf_id);
            leaf.value_blob = value_blob;
            leaf.value_len = u32::try_from(value.len()).expect("value too long");
            return;
        }

        // Existing leaf key bytes.
        let old_key = {
            let bytes = self.pages.slot_bytes_mut(old_key_blob);
            bytes[..old_key_len].to_vec()
        };

        let mut i = depth;
        while i < old_key.len() && i < key.len() && old_key[i] == key[i] {
            i += 1;
        }
        let shared = i - depth;

        let new_leaf = self.alloc_leaf(key, value);
        let top = self.branch_from_shared(depth, shared, &old_key, key, leaf_id, new_leaf);
        self.replace_child(parent, leaf_id, top);
    }

    /// Builds a compressed path for `shared` bytes starting at `depth`, ending
    /// in a branching node that holds `old_leaf` and `new_leaf`.
    ///
    /// When `shared > MAX_PREFIX`, emits a spine of unary nodes each carrying
    /// a full inline prefix, so search depth advances by `MAX_PREFIX + 1` per
    /// hop instead of one byte. The diverge byte is always taken relative to
    /// the full shared length (never relative to a truncated prefix alone).
    fn branch_from_shared(
        &mut self,
        depth: usize,
        shared: usize,
        old_key: &[u8],
        key: &[u8],
        old_leaf: AllocId,
        new_leaf: AllocId,
    ) -> AllocId {
        let mut d = depth;
        let mut rem = shared;
        let mut top: Option<AllocId> = None;
        let mut link: Option<(AllocId, u8)> = None;

        while rem > MAX_PREFIX {
            let node = self.alloc_node4_prefixed(&key[d..d + MAX_PREFIX]);
            let edge = key[d + MAX_PREFIX];
            match link {
                None => top = Some(node),
                Some((p, b)) => self.add_child_to(p, b, node),
            }
            link = Some((node, edge));
            d += MAX_PREFIX + 1;
            rem -= MAX_PREFIX + 1;
        }

        let branch = self.alloc_node4_prefixed(&key[d..d + rem]);
        match link {
            None => top = Some(branch),
            Some((p, b)) => self.add_child_to(p, b, branch),
        }

        let diverge = depth + shared;
        if diverge < old_key.len() && diverge < key.len() {
            self.add_child_to(branch, old_key[diverge], old_leaf);
            self.add_child_to(branch, key[diverge], new_leaf);
        } else if diverge < key.len() {
            // `old_key` is a prefix of `key`.
            self.add_child_to(branch, key[diverge], new_leaf);
            self.add_child_to(branch, old_key.get(diverge).copied().unwrap_or(0), old_leaf);
        } else if diverge < old_key.len() {
            // `key` is a prefix of `old_key`.
            self.add_child_to(branch, old_key[diverge], old_leaf);
            self.add_child_to(branch, key.get(diverge).copied().unwrap_or(0), new_leaf);
        }

        top.expect("branch_from_shared always allocates a node")
    }

    fn alloc_node4_prefixed(&mut self, prefix: &[u8]) -> AllocId {
        assert!(prefix.len() <= MAX_PREFIX);
        let mut header = NodeHeader {
            tag: Tag::Node4 as u8,
            count: 0,
            prefix_len: u8::try_from(prefix.len()).expect("prefix fits u8"),
            _pad: 0,
            prefix: [0; MAX_PREFIX],
        };
        header.prefix[..prefix.len()].copy_from_slice(prefix);
        let n4 = TypedAlloc::<Node4>::alloc(&mut self.pages).expect("alloc node4");
        *n4.get_mut(&mut self.pages) = Node4 {
            header,
            keys: [0; 4],
            children: [AllocId::invalid(); 4],
        };
        n4.id()
    }

    fn insert_split_prefix(
        &mut self,
        node: AllocId,
        parent: Option<(AllocId, u8)>,
        depth: usize,
        mismatch: usize,
        key: &[u8],
        value: &[u8],
    ) {
        let (old_prefix_len, old_prefix, old_tag) = {
            let bytes = self.pages.slot_bytes_mut(node);
            let plen = usize::from(bytes[2]);
            let mut p = [0u8; MAX_PREFIX];
            p.copy_from_slice(&bytes[4..4 + MAX_PREFIX]);
            (plen, p, bytes[0])
        };
        let _ = old_tag;

        // Shrink old node's prefix.
        let rest = old_prefix_len - mismatch - 1;
        let split_byte = old_prefix[mismatch];
        {
            let bytes = self.pages.slot_bytes_mut(node);
            bytes[2] = u8::try_from(rest).unwrap();
            let mut np = [0u8; MAX_PREFIX];
            if rest > 0 {
                np[..rest].copy_from_slice(&old_prefix[mismatch + 1..mismatch + 1 + rest]);
            }
            bytes[4..4 + MAX_PREFIX].copy_from_slice(&np);
        }

        let leaf = self.alloc_leaf(key, value);
        let n4_id = self.alloc_node4_prefixed(&old_prefix[..mismatch]);
        self.add_child_to(n4_id, split_byte, node);
        self.add_child_to(n4_id, key[depth + mismatch], leaf);
        self.replace_child(parent, node, n4_id);
    }

    fn replace_child(&mut self, parent: Option<(AllocId, u8)>, old: AllocId, new: AllocId) {
        match parent {
            None => {
                if self.root == Some(old) {
                    self.root = Some(new);
                }
            }
            Some((p, byte)) => {
                self.set_child(p, byte, new);
            }
        }
    }

    fn set_child(&mut self, node: AllocId, byte: u8, child: AllocId) {
        match self.tag(node) {
            t if t == Tag::Node4 as u8 => {
                let n = self.pages.get_mut::<Node4>(node);
                for i in 0..usize::from(n.header.count) {
                    if n.keys[i] == byte {
                        n.children[i] = child;
                        return;
                    }
                }
            }
            t if t == Tag::Node16 as u8 => {
                let n = self.pages.get_mut::<Node16>(node);
                for i in 0..usize::from(n.header.count) {
                    if n.keys[i] == byte {
                        n.children[i] = child;
                        return;
                    }
                }
            }
            t if t == Tag::Node48 as u8 => {
                let n = self.pages.get_mut::<Node48>(node);
                let idx = n.keys[usize::from(byte)];
                if idx != 0 {
                    n.children[usize::from(idx) - 1] = child;
                }
            }
            t if t == Tag::Node256 as u8 => {
                self.pages.get_mut::<Node256>(node).children[usize::from(byte)] = child;
            }
            _ => {}
        }
    }

    fn add_child_to(&mut self, node: AllocId, byte: u8, child: AllocId) {
        match self.tag(node) {
            t if t == Tag::Node4 as u8 => {
                let count = usize::from(self.pages.get_mut::<Node4>(node).header.count);
                if count < 4 {
                    let n = self.pages.get_mut::<Node4>(node);
                    n.keys[count] = byte;
                    n.children[count] = child;
                    n.header.count += 1;
                } else {
                    let grown = self.grow_node4(node);
                    self.add_child_to(grown, byte, child);
                }
            }
            t if t == Tag::Node16 as u8 => {
                let count = usize::from(self.pages.get_mut::<Node16>(node).header.count);
                if count < 16 {
                    let n = self.pages.get_mut::<Node16>(node);
                    n.keys[count] = byte;
                    n.children[count] = child;
                    n.header.count += 1;
                } else {
                    let grown = self.grow_node16(node);
                    self.add_child_to(grown, byte, child);
                }
            }
            t if t == Tag::Node48 as u8 => {
                let count = usize::from(self.pages.get_mut::<Node48>(node).header.count);
                if count < 48 {
                    let n = self.pages.get_mut::<Node48>(node);
                    n.children[count] = child;
                    n.keys[usize::from(byte)] = u8::try_from(count + 1).unwrap();
                    n.header.count += 1;
                } else {
                    let grown = self.grow_node48(node);
                    self.add_child_to(grown, byte, child);
                }
            }
            t if t == Tag::Node256 as u8 => {
                let n = self.pages.get_mut::<Node256>(node);
                if !n.children[usize::from(byte)].is_valid() {
                    n.header.count += 1;
                }
                n.children[usize::from(byte)] = child;
            }
            _ => {}
        }
    }

    fn grow_node4(&mut self, node: AllocId) -> AllocId {
        let old = *self.pages.get_mut::<Node4>(node);
        let n16 = TypedAlloc::<Node16>::alloc(&mut self.pages).expect("alloc node16");
        {
            let n = n16.get_mut(&mut self.pages);
            *n = Node16 {
                header: NodeHeader {
                    tag: Tag::Node16 as u8,
                    count: old.header.count,
                    prefix_len: old.header.prefix_len,
                    _pad: 0,
                    prefix: old.header.prefix,
                },
                keys: [0; 16],
                _pad: [0; 4],
                children: [AllocId::invalid(); 16],
            };
            for i in 0..usize::from(old.header.count) {
                n.keys[i] = old.keys[i];
                n.children[i] = old.children[i];
            }
        }
        let id = n16.id();
        self.rewire_parent_of(node, id);
        self.pages.dealloc(node);
        id
    }

    fn grow_node16(&mut self, node: AllocId) -> AllocId {
        let old = *self.pages.get_mut::<Node16>(node);
        let n48 = TypedAlloc::<Node48>::alloc(&mut self.pages).expect("alloc node48");
        {
            let n = n48.get_mut(&mut self.pages);
            *n = Node48 {
                header: NodeHeader {
                    tag: Tag::Node48 as u8,
                    count: old.header.count,
                    prefix_len: old.header.prefix_len,
                    _pad: 0,
                    prefix: old.header.prefix,
                },
                keys: [0; 256],
                _pad: [0; 4],
                children: [AllocId::invalid(); 48],
            };
            for i in 0..usize::from(old.header.count) {
                n.children[i] = old.children[i];
                n.keys[usize::from(old.keys[i])] = u8::try_from(i + 1).unwrap();
            }
        }
        let id = n48.id();
        self.rewire_parent_of(node, id);
        self.pages.dealloc(node);
        id
    }

    fn grow_node48(&mut self, node: AllocId) -> AllocId {
        let old = *self.pages.get_mut::<Node48>(node);
        let n256 = TypedAlloc::<Node256>::alloc(&mut self.pages).expect("alloc node256");
        {
            let n = n256.get_mut(&mut self.pages);
            *n = Node256 {
                header: NodeHeader {
                    tag: Tag::Node256 as u8,
                    count: old.header.count,
                    prefix_len: old.header.prefix_len,
                    _pad: 0,
                    prefix: old.header.prefix,
                },
                _pad: [0; 4],
                children: [AllocId::invalid(); 256],
            };
            for b in 0..256 {
                let idx = old.keys[b];
                if idx != 0 {
                    n.children[b] = old.children[usize::from(idx) - 1];
                }
            }
        }
        let id = n256.id();
        self.rewire_parent_of(node, id);
        self.pages.dealloc(node);
        id
    }

    /// After growing `old` into `new`, update root or search again — we only
    /// know `old` id; scan is avoided by updating root if needed and relying on
    /// caller re-fetch. For grow during `add_child_to`, parent still points at
    /// `old`. Walk from root to replace.
    fn rewire_parent_of(&mut self, old: AllocId, new: AllocId) {
        if self.root == Some(old) {
            self.root = Some(new);
            return;
        }
        if let Some(root) = self.root {
            self.rewire_in_subtree(root, old, new);
        }
    }

    fn rewire_in_subtree(&mut self, node: AllocId, old: AllocId, new: AllocId) -> bool {
        let tag = self.tag(node);
        if tag == Tag::Leaf as u8 {
            return false;
        }
        // Collect children first (avoid borrow issues).
        let children: Vec<(u8, AllocId)> = match tag {
            t if t == Tag::Node4 as u8 => {
                let n = *self.pages.get_mut::<Node4>(node);
                (0..usize::from(n.header.count))
                    .filter(|&i| n.children[i].is_valid())
                    .map(|i| (n.keys[i], n.children[i]))
                    .collect()
            }
            t if t == Tag::Node16 as u8 => {
                let n = *self.pages.get_mut::<Node16>(node);
                (0..usize::from(n.header.count))
                    .filter(|&i| n.children[i].is_valid())
                    .map(|i| (n.keys[i], n.children[i]))
                    .collect()
            }
            t if t == Tag::Node48 as u8 => {
                let n = *self.pages.get_mut::<Node48>(node);
                (0..=255u8)
                    .filter_map(|b| {
                        let idx = n.keys[usize::from(b)];
                        if idx == 0 {
                            None
                        } else {
                            let c = n.children[usize::from(idx) - 1];
                            c.is_valid().then_some((b, c))
                        }
                    })
                    .collect()
            }
            t if t == Tag::Node256 as u8 => {
                let n = *self.pages.get_mut::<Node256>(node);
                (0..=255u8)
                    .filter_map(|b| {
                        let c = n.children[usize::from(b)];
                        c.is_valid().then_some((b, c))
                    })
                    .collect()
            }
            _ => Vec::new(),
        };

        for (byte, child) in children {
            if child == old {
                self.set_child(node, byte, new);
                return true;
            }
            if self.rewire_in_subtree(child, old, new) {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("arttrie-{name}-{n}.bin"))
    }

    #[test]
    fn insert_get_roundtrip() {
        let path = tmp_path("roundtrip");
        let mut trie = ArtTrie::open(&path).expect("open");
        trie.insert(b"hello", b"world");
        trie.insert(b"help", b"desk");
        trie.insert(b"helium", b"gas");

        assert_eq!(trie.get(b"hello").value, Some(b"world".as_slice()));
        assert_eq!(trie.get(b"help").value, Some(b"desk".as_slice()));
        assert_eq!(trie.get(b"helium").value, Some(b"gas".as_slice()));
        assert_eq!(trie.get(b"missing").value, None);

        trie.insert(b"hello", b"WORLD");
        assert_eq!(trie.get(b"hello").value, Some(b"WORLD".as_slice()));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn long_hash_prefix_compresses_and_looks_up() {
        let path = tmp_path("hash-prefix");
        let mut trie = ArtTrie::open(&path).expect("open");
        let a = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let b = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab";
        let c = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaac";
        trie.insert(a, b"A");
        trie.insert(b, b"B");
        trie.insert(c, b"C");

        assert_eq!(trie.get(a).value, Some(b"A".as_slice()));
        assert_eq!(trie.get(b).value, Some(b"B".as_slice()));
        assert_eq!(trie.get(c).value, Some(b"C".as_slice()));

        let hit = trie.get(a);
        assert!(hit.found());
        // Compressed path: root Node4 (prefix) → leaf. Two nodes, one edge,
        // ~39 prefix bytes matched in a single inner hop.
        assert_eq!(hit.stats.nodes_visited, 2);
        assert_eq!(hit.stats.inner_nodes_visited, 1);
        assert_eq!(hit.stats.edges_followed, 1);
        assert_eq!(hit.stats.prefix_bytes_matched, 39);
        assert!(hit.stats.leaf_compared);

        let dump = trie.debug_dump_with_pages(false);
        // One node holds the 39-byte shared run; no byte-at-a-time spine.
        assert!(
            dump.contains("prefix=\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\""),
            "expected full 39-a prefix compression, got:\n{dump}"
        );
        assert!(
            !dump.contains("prefix=\"aaaaaaaa\"\n"),
            "should not truncate to 8-byte chunks:\n{dump}"
        );
        let _ = std::fs::remove_file(path);
    }
}
