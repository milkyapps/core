//! Memory-mapped pool of allocators.
//!
//! Layout of the backing file:
//!
//! ```text
//! ┌──────────────────────────── page 0 ────────────────────────────┐
//! │ MetadataPage (page_size, free_list_by_size[32], flush thresh)  │
//! └────────────────────────────────────────────────────────────────┘
//! ┌──────────────────────────── page N≥1 ──────────────────────────┐
//! │ [prev][next][class_id][bitmap…][pad][slot 0][slot 1]…          │
//! └────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Each size class `i` (objects of size `2^i`) owns a doubly linked list of
//! data pages threaded through `MetadataPage::free_list_by_size[i]`. Within a
//! page, a **variable-length** bitmap tracks which slots are occupied (`1`) or
//! free (`0`). `slot_count` / `bitmap_bytes` are **not** stored: they are
//! derived from the file’s logical `page_size` and the page’s `class_id`.
//!
//! # Logical page size vs OS page size
//!
//! The store is carved into fixed-size **logical** pages whose byte length is
//! stored in the on-disk metadata (`page_size`) and chosen when the file is
//! created (see [`DEFAULT_PAGE_SIZE`]). That value is part of the file format;
//! it is **not** required to equal the host VM page size
//! (`sysconf(_SC_PAGESIZE)` / `vm_page_size`), which is often 4 KiB on
//! Linux/Windows/Intel macOS and **16 KiB on Apple Silicon**.
//!
//! A file created with 4 KiB logical pages therefore loads correctly on a
//! machine whose OS pages are 16 KiB (and vice versa): reopen reads the
//! on-disk `page_size` and validates the file length against it. Matching the
//! OS page size can still help mmap/`flush_range` efficiency, but it is not
//! required for correctness or portability of the file format.

use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::io;
use std::mem::{align_of, size_of};
use std::path::Path;

/// Default **logical** page size written into newly created store files (4 KiB).
///
/// This is a file-format slab size, not “the OS page size of the current host.”
/// Host VM pages may be 4 KiB or 16 KiB (notably Apple Silicon); see the module
/// docs. Existing files keep whatever `page_size` was stored in their metadata
/// when they were created, so stores remain portable across those hosts.
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// Sentinel page index meaning “no page” (page 0 is always metadata).
const NO_PAGE: usize = 0;

/// Opaque index of a page in the mmap file (`0` = metadata page).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PageId(usize);

/// Index of a slot inside a data page.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SlotId(usize);

/// Handle returned by [`PageManager::alloc`], identifying a live object slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AllocId {
    /// Power-of-two slot size (bytes) used for this allocation.
    size: usize,
    page_id: PageId,
    slot_id: SlotId,
}

impl AllocId {
    /// Power-of-two slot size reserved for this allocation.
    #[must_use]
    pub const fn size(&self) -> usize {
        self.size
    }

    /// Page that owns the slot.
    #[must_use]
    pub const fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Slot index within the page.
    #[must_use]
    pub const fn slot_id(&self) -> SlotId {
        self.slot_id
    }
}

/// On-disk / in-mmap header stored at page 0.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MetadataPage {
    /// Logical page size in bytes for this file (power of two).
    ///
    /// Chosen at create time and authoritative on reopen. Independent of the
    /// host OS/VM page size; see the module-level docs.
    page_size: usize,
    /// Head of the free-page list for objects sized `2^i`.
    ///
    /// Each entry can point to **any** data page of that size class (they
    /// form a linked list). Prefer the page with the most free slots when
    /// updating.
    free_list_by_size: [usize; 32],
    /// Flush dirty pages once `dirty.len()` reaches this threshold.
    dirties_before_flush: usize,
}

/// Fixed prefix of every data page, before the variable-length occupancy bitmap.
///
/// Full page layout:
/// `[previous][next][class_id][_pad][bitmap…][align pad][slot 0]…`
///
/// `slot_count` and `bitmap_bytes` are derived from `page_size` + `class_id`
/// (see [`page_layout`]); they are not stored on disk.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct DataPageHeader {
    previous: usize,
    next: usize,
    /// Size-class index `i` where slot size is `2^i`.
    class_id: u32,
    /// Reserved; keeps the header 8-byte aligned on 64-bit hosts.
    _pad: u32,
}

/// Derived geometry of a data page for a given logical `page_size` and class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PageLayout {
    class_id: usize,
    slot_size: usize,
    slot_count: usize,
    bitmap_bytes: usize,
    /// Byte offset from the start of the page to slot 0.
    payload_off: usize,
}

/// Pool of size-classed slab allocators over a single mmap'd file.
pub struct PageManager {
    file: File,
    mmap: MmapMut,
    /// Cached pointer into `mmap` at offset 0 (invalidated on remap).
    meta: *mut MetadataPage,
    dirty: Vec<PageId>,
}

// `meta` aliases `mmap`; the manager is not thread-safe.
unsafe impl Send for PageManager {}

impl PageManager {
    /// Opens `path`, creating and initializing an empty store if needed.
    ///
    /// New files use [`DEFAULT_PAGE_SIZE`] as their logical page size. Existing
    /// files keep the `page_size` already stored in their metadata (portable
    /// across hosts with different OS/VM page sizes).
    ///
    /// Returns `None` if the file cannot be opened, sized, or mapped.
    #[must_use]
    pub fn open(path: impl AsRef<Path>) -> Option<Self> {
        Self::open_with_options(path, DEFAULT_PAGE_SIZE, 16).ok()
    }

    /// Like [`Self::open`], but returns the underlying [`io::Error`].
    ///
    /// `page_size` is the **logical** slab size used only when creating a new
    /// file; it is ignored for an existing store (the on-disk value wins). It
    /// need not match the host OS page size.
    ///
    /// `dirties_before_flush` controls how many dirty pages may accumulate
    /// before [`Self::alloc`] / [`Self::dealloc`] trigger a flush.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created, resized, mapped, or if an
    /// existing file has an invalid layout.
    ///
    /// # Panics
    ///
    /// Panics if `page_size` is not a power of two, is too small to hold the
    /// on-disk headers / at least one 8-byte slot, or if `dirties_before_flush`
    /// is zero.
    pub fn open_with_options(
        path: impl AsRef<Path>,
        page_size: usize,
        dirties_before_flush: usize,
    ) -> io::Result<Self> {
        assert!(
            page_size.is_power_of_two(),
            "page_size must be a power of two"
        );
        assert!(
            page_size >= size_of::<MetadataPage>(),
            "page_size too small for MetadataPage"
        );
        assert!(
            page_layout(page_size, size_class(8)).is_some_and(|l| l.slot_count >= 1),
            "page_size too small for a data page with at least one 8-byte slot"
        );
        assert!(dirties_before_flush > 0);

        let path = path.as_ref();
        let is_new = !path.exists();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        if is_new {
            file.set_len(page_size as u64)?;
        } else if file.metadata()?.len() < size_of::<MetadataPage>() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file smaller than MetadataPage",
            ));
        }

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        // Pages are page_size-aligned in the file; MetadataPage is at offset 0.
        #[allow(clippy::cast_ptr_alignment)]
        let meta = mmap.as_mut_ptr().cast::<MetadataPage>();

        if is_new {
            // SAFETY: mmap is at least one page; MetadataPage fits in page 0.
            unsafe {
                meta.write(MetadataPage {
                    page_size,
                    free_list_by_size: [NO_PAGE; 32],
                    dirties_before_flush,
                });
            }
            mmap.flush()?;
        } else {
            // SAFETY: file is large enough to contain a MetadataPage at offset 0.
            let on_disk_page_size = unsafe { (*meta).page_size };
            if on_disk_page_size == 0 || !on_disk_page_size.is_power_of_two() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid on-disk page_size",
                ));
            }
            let len = mmap.len();
            if len < on_disk_page_size || !len.is_multiple_of(on_disk_page_size) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "file size is not a multiple of on-disk page_size",
                ));
            }
        }

        Ok(Self {
            file,
            mmap,
            meta,
            dirty: Vec::new(),
        })
    }

    /// Number of pages currently mapped (including the metadata page).
    #[must_use]
    pub fn page_count(&self) -> usize {
        self.mmap.len() / self.page_size()
    }

    /// Logical page size in bytes for this store (from on-disk metadata).
    ///
    /// This is the file-format slab size, not necessarily the host OS/VM page
    /// size. See the module docs.
    #[must_use]
    pub fn page_size(&self) -> usize {
        // SAFETY: meta always points at page 0 of a live mmap.
        unsafe { (*self.meta).page_size }
    }

    /// Writes every mapped byte through to the underlying file.
    ///
    /// # Errors
    ///
    /// Returns an error if the OS cannot sync the mapping to disk.
    pub fn flush_all(&mut self) -> io::Result<()> {
        self.mmap.flush()?;
        self.dirty.clear();
        Ok(())
    }

    /// Flushes a single page to the file.
    ///
    /// # Errors
    ///
    /// Returns an error if `page` is out of range or the OS cannot sync the
    /// corresponding byte range.
    pub fn flush_page(&mut self, page: PageId) -> io::Result<()> {
        let page_size = self.page_size();
        let start = page
            .0
            .checked_mul(page_size)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "page offset overflow"))?;
        if start + page_size > self.mmap.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "page id out of range",
            ));
        }
        self.mmap.flush_range(start, page_size)?;
        self.dirty.retain(|p| *p != page);
        Ok(())
    }

    /// Flushes all pages recorded as dirty and clears the dirty set.
    ///
    /// # Errors
    ///
    /// Returns an error if flushing any dirty page fails.
    pub fn flush_dirty(&mut self) -> io::Result<()> {
        let pages: Vec<PageId> = self.dirty.clone();
        for page in pages {
            self.flush_page(page)?;
        }
        self.dirty.clear();
        Ok(())
    }

    /// Allocates a slot large enough for `T` and returns its [`AllocId`].
    ///
    /// Returns `None` if `T` cannot fit in a data page (header + one aligned
    /// slot would exceed the page size).
    ///
    /// The slot memory is left uninitialized. Use [`Self::get_mut`] to write.
    pub fn alloc<T: Copy>(&mut self) -> Option<AllocId> {
        let size = size_of::<T>().max(1);
        let align = align_of::<T>().max(1);
        let slot_size = size.next_power_of_two().max(align.next_power_of_two());
        self.alloc_slot(slot_size)
    }

    /// Allocates one slot of exactly `slot_size` bytes (`slot_size` must be a
    /// power of two).
    ///
    /// # Panics
    ///
    /// Panics if `slot_size` is zero or not a power of two.
    pub fn alloc_slot(&mut self, slot_size: usize) -> Option<AllocId> {
        assert!(slot_size.is_power_of_two() && slot_size > 0);
        let class = size_class(slot_size);
        let layout = page_layout(self.page_size(), class)?;

        // Walk the size-class list looking for a free bit.
        let head = self.meta_ref().free_list_by_size[class];
        let mut page_id = head;
        while page_id != NO_PAGE {
            if let Some(slot) = self.find_free_slot(PageId(page_id), &layout) {
                self.mark_slot_used(PageId(page_id), &layout, slot);
                self.mark_dirty(PageId(page_id));
                self.maybe_flush_by_threshold();
                return Some(AllocId {
                    size: slot_size,
                    page_id: PageId(page_id),
                    slot_id: slot,
                });
            }
            page_id = self.header_ref(PageId(page_id)).next;
        }

        // No free slot — grow the file and link a fresh page at the list head.
        let new_page = self.alloc_free_page(slot_size)?;
        let slot = self
            .find_free_slot(new_page, &layout)
            .expect("fresh page must have a free slot");
        self.mark_slot_used(new_page, &layout, slot);
        self.mark_dirty(new_page);
        self.mark_dirty(PageId(0)); // free_list_by_size updated
        self.maybe_flush_by_threshold();
        Some(AllocId {
            size: slot_size,
            page_id: new_page,
            slot_id: slot,
        })
    }

    /// Releases a previously allocated slot.
    ///
    /// Sets the bitmap bit of the slot to `0` (free), marks the page dirty, and
    /// may flush when the dirty threshold is exceeded.
    pub fn dealloc(&mut self, aid: AllocId) {
        debug_assert!(aid.page_id.0 != NO_PAGE, "cannot dealloc metadata page");
        let layout = page_layout(self.page_size(), size_class(aid.size))
            .expect("dealloc of a size class that cannot fit in a page");
        debug_assert!(aid.slot_id.0 < layout.slot_count, "slot out of range");

        self.mark_slot_free(aid.page_id, &layout, aid.slot_id);
        // Prefer this page for future allocs of the same class (it has a free slot).
        let class = size_class(aid.size);
        self.meta_mut().free_list_by_size[class] = aid.page_id.0;
        self.mark_dirty(aid.page_id);
        self.mark_dirty(PageId(0));
        self.maybe_flush_by_threshold();
    }

    /// Returns a mutable reference to the object stored at `aid`.
    ///
    /// # Panics
    ///
    /// Panics if `T` does not fit in the allocation's size class.
    ///
    /// # Safety contract
    ///
    /// `T` must match the type (size/align) used when `aid` was allocated.
    /// The returned reference is valid until the next method that remaps the
    /// file (`alloc` that grows the file, or drop).
    pub fn get_mut<T: Copy>(&mut self, aid: AllocId) -> &mut T {
        let needed = size_of::<T>()
            .max(1)
            .next_power_of_two()
            .max(align_of::<T>().next_power_of_two());
        assert!(
            needed <= aid.size,
            "get_mut type does not fit allocation size class (need {needed}, have {})",
            aid.size
        );
        let ptr = self.slot_ptr::<T>(aid);
        // SAFETY: slot was reserved by alloc for this size class; caller
        // guarantees T matches the allocation.
        unsafe { &mut *ptr }
    }

    /// Raw pointer to the slot for `aid` (same validity rules as [`Self::get_mut`]).
    #[must_use]
    pub fn slot_ptr<T>(&self, aid: AllocId) -> *mut T {
        let page_size = self.page_size();
        let layout = page_layout(page_size, size_class(aid.size))
            .expect("slot_ptr for a size class that cannot fit in a page");
        let offset = aid.page_id.0 * page_size + layout.payload_off + aid.slot_id.0 * aid.size;
        // SAFETY: offset is within the mapped file for a valid AllocId.
        // Payload start is aligned to `aid.size` (power of two).
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            self.mmap.as_ptr().add(offset) as *mut T
        }
    }

    /// Mutable byte slice covering the full slot reserved by `aid`.
    pub fn slot_bytes_mut(&mut self, aid: AllocId) -> &mut [u8] {
        let ptr = self.slot_ptr::<u8>(aid);
        // SAFETY: alloc reserved `aid.size` bytes at this address.
        unsafe { std::slice::from_raw_parts_mut(ptr, aid.size) }
    }

    /// Human-readable dump of metadata, free lists, dirty set, and every page.
    ///
    /// Intended for deterministic snapshot tests (see `tests/main.rs`).
    #[must_use]
    pub fn debug_dump(&self) -> String {
        let mut out = String::new();
        let page_size = self.page_size();
        let page_count = self.page_count();
        let meta = self.meta_ref();

        let mut dirty: Vec<_> = self.dirty.iter().map(|p| p.0).collect();
        dirty.sort_unstable();

        out.push_str(&format!("page_count: {page_count}\n"));
        out.push_str(&format!("page_size: {page_size}\n"));
        out.push_str(&format!("dirty: {dirty:?}\n"));
        out.push_str(&format!(
            "dirties_before_flush: {}\n",
            meta.dirties_before_flush
        ));

        out.push_str("free_lists:\n");
        let mut any_list = false;
        for (class, &head) in meta.free_list_by_size.iter().enumerate() {
            if head == NO_PAGE {
                continue;
            }
            any_list = true;
            let slot_size = 1usize << class;
            let chain = self.page_chain(head);
            out.push_str(&format!("  class {class} ({slot_size}B): {chain:?}\n"));
        }
        if !any_list {
            out.push_str("  (none)\n");
        }

        for pid in 0..page_count {
            if pid == NO_PAGE {
                out.push_str("\n=== page 0 (metadata) ===\n");
                out.push_str(&format!(
                    "dirties_before_flush: {}\n",
                    meta.dirties_before_flush
                ));
                continue;
            }

            let hdr = self.header_ref(PageId(pid));
            let class = hdr.class_id as usize;
            let Some(layout) = page_layout(page_size, class) else {
                out.push_str(&format!(
                    "\n=== page {pid} (data, class={class}, invalid layout) ===\n"
                ));
                continue;
            };

            out.push_str(&format!(
                "\n=== page {pid} (data, class={class}, slot_size={}, slots={}, bitmap_bytes={}) ===\n",
                layout.slot_size, layout.slot_count, layout.bitmap_bytes
            ));
            out.push_str(&format!("prev: {}  next: {}\n", hdr.previous, hdr.next));

            let bitmap = self.bitmap_ref(PageId(pid), &layout);
            let mut occupied = Vec::new();
            for s in 0..layout.slot_count {
                if bit_is_set(bitmap, s) {
                    occupied.push(s);
                }
            }
            out.push_str(&format!("occupied_slots: {occupied:?}\n"));

            for s in occupied {
                let offset = pid * page_size + layout.payload_off + s * layout.slot_size;
                let bytes = &self.mmap[offset..offset + layout.slot_size];
                out.push_str(&format!("slot[{s}]: {}\n", format_slot_bytes(bytes)));
            }
        }

        out
    }

    /// Page indices in the linked list starting at `head` (follows `next`).
    fn page_chain(&self, head: usize) -> Vec<usize> {
        let mut chain = Vec::new();
        let mut cur = head;
        let mut guard = 0;
        while cur != NO_PAGE && guard < self.page_count() + 1 {
            chain.push(cur);
            cur = self.header_ref(PageId(cur)).next;
            guard += 1;
        }
        chain
    }

    // ── internals ──────────────────────────────────────────────────────────

    fn meta_ref(&self) -> &MetadataPage {
        // SAFETY: meta aliases page 0 for the lifetime of self.
        unsafe { &*self.meta }
    }

    fn meta_mut(&mut self) -> &mut MetadataPage {
        // SAFETY: exclusive borrow of self; meta aliases page 0.
        unsafe { &mut *self.meta }
    }

    fn page_byte_offset(&self, page: PageId) -> usize {
        debug_assert!(page.0 != NO_PAGE);
        page.0 * self.page_size()
    }

    fn header_ref(&self, page: PageId) -> &DataPageHeader {
        let offset = self.page_byte_offset(page);
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &*self.mmap.as_ptr().add(offset).cast::<DataPageHeader>()
        }
    }

    fn header_mut(&mut self, page: PageId) -> &mut DataPageHeader {
        let offset = self.page_byte_offset(page);
        #[allow(clippy::cast_ptr_alignment)]
        unsafe {
            &mut *self.mmap.as_mut_ptr().add(offset).cast::<DataPageHeader>()
        }
    }

    fn bitmap_ref<'a>(&'a self, page: PageId, layout: &PageLayout) -> &'a [u8] {
        let offset = self.page_byte_offset(page) + size_of::<DataPageHeader>();
        &self.mmap[offset..offset + layout.bitmap_bytes]
    }

    fn bitmap_mut<'a>(&'a mut self, page: PageId, layout: &PageLayout) -> &'a mut [u8] {
        let offset = self.page_byte_offset(page) + size_of::<DataPageHeader>();
        &mut self.mmap[offset..offset + layout.bitmap_bytes]
    }

    #[cfg(test)]
    fn slots_per_page(&self, slot_size: usize) -> usize {
        page_layout(self.page_size(), size_class(slot_size))
            .map_or(0, |l| l.slot_count)
    }

    /// Appends a new zeroed data page for `slot_size` and links it at the head
    /// of that size class's free list.
    fn alloc_free_page(&mut self, slot_size: usize) -> Option<PageId> {
        let page_size = self.page_size();
        let class = size_class(slot_size);
        page_layout(page_size, class)?;
        let new_id = self.page_count();
        let new_len = (new_id + 1).checked_mul(page_size)?;

        // Grow the file, then remap so the new page is visible.
        self.mmap.flush().ok()?;
        self.file.set_len(new_len as u64).ok()?;
        // Remap: previous pointers into mmap are invalid after this.
        self.mmap = unsafe { MmapMut::map_mut(&self.file).ok()? };
        #[allow(clippy::cast_ptr_alignment)]
        {
            self.meta = self.mmap.as_mut_ptr().cast::<MetadataPage>();
        }

        let page = PageId(new_id);
        let old_head = self.meta_ref().free_list_by_size[class];

        // Zero the whole logical page (header + bitmap + slots).
        let start = new_id * page_size;
        self.mmap[start..start + page_size].fill(0);

        {
            let hdr = self.header_mut(page);
            hdr.previous = NO_PAGE;
            hdr.next = old_head;
            hdr.class_id = u32::try_from(class).expect("class_id fits u32");
            hdr._pad = 0;
        }

        if old_head != NO_PAGE {
            self.header_mut(PageId(old_head)).previous = new_id;
        }
        self.meta_mut().free_list_by_size[class] = new_id;
        Some(page)
    }

    fn find_free_slot(&self, page: PageId, layout: &PageLayout) -> Option<SlotId> {
        let bitmap = self.bitmap_ref(page, layout);
        for i in 0..layout.slot_count {
            if !bit_is_set(bitmap, i) {
                return Some(SlotId(i));
            }
        }
        None
    }

    fn mark_slot_used(&mut self, page: PageId, layout: &PageLayout, slot: SlotId) {
        let bitmap = self.bitmap_mut(page, layout);
        set_bit(bitmap, slot.0);
    }

    fn mark_slot_free(&mut self, page: PageId, layout: &PageLayout, slot: SlotId) {
        let bitmap = self.bitmap_mut(page, layout);
        clear_bit(bitmap, slot.0);
    }

    fn mark_dirty(&mut self, page: PageId) {
        if !self.dirty.contains(&page) {
            self.dirty.push(page);
        }
    }

    fn maybe_flush_by_threshold(&mut self) {
        let threshold = self.meta_ref().dirties_before_flush;
        if self.dirty.len() >= threshold {
            let _ = self.flush_dirty();
        }
    }
}

impl Drop for PageManager {
    fn drop(&mut self) {
        let _ = self.flush_all();
    }
}

/// Size-class index `i` for a power-of-two `slot_size` (`2^i`).
fn size_class(slot_size: usize) -> usize {
    debug_assert!(slot_size.is_power_of_two());
    slot_size.trailing_zeros() as usize
}

/// Computes data-page geometry for `page_size` + size `class_id`.
///
/// Finds the largest `slot_count` such that
/// `sizeof(DataPageHeader) + ceil(slot_count/8) + align_pad + slot_count * slot_size ≤ page_size`.
///
/// Returns `None` if not even one slot fits.
fn page_layout(page_size: usize, class_id: usize) -> Option<PageLayout> {
    if class_id >= usize::BITS as usize {
        return None;
    }
    let slot_size = 1usize << class_id;
    let fixed = size_of::<DataPageHeader>();
    if fixed >= page_size {
        return None;
    }

    // Binary search the maximum n that fits.
    let mut lo = 0usize;
    let mut hi = (page_size - fixed) / slot_size + 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        if layout_fits(page_size, slot_size, mid) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }

    if lo == 0 {
        return None;
    }

    let bitmap_bytes = lo.div_ceil(8);
    let prefix = fixed + bitmap_bytes;
    let payload_off = prefix.div_ceil(slot_size) * slot_size;
    Some(PageLayout {
        class_id,
        slot_size,
        slot_count: lo,
        bitmap_bytes,
        payload_off,
    })
}

fn layout_fits(page_size: usize, slot_size: usize, slot_count: usize) -> bool {
    let bitmap_bytes = slot_count.div_ceil(8);
    let prefix = size_of::<DataPageHeader>()
        .checked_add(bitmap_bytes)
        .unwrap_or(usize::MAX);
    let payload_off = prefix.div_ceil(slot_size).saturating_mul(slot_size);
    let Some(total) = payload_off.checked_add(slot_count.saturating_mul(slot_size)) else {
        return false;
    };
    total <= page_size
}

fn bit_is_set(bitmap: &[u8], bit: usize) -> bool {
    let byte = bit / 8;
    let mask = 1u8 << (bit % 8);
    bitmap[byte] & mask != 0
}

fn set_bit(bitmap: &mut [u8], bit: usize) {
    let byte = bit / 8;
    let mask = 1u8 << (bit % 8);
    bitmap[byte] |= mask;
}

fn clear_bit(bitmap: &mut [u8], bit: usize) {
    let byte = bit / 8;
    let mask = 1u8 << (bit % 8);
    bitmap[byte] &= !mask;
}

fn format_slot_bytes(bytes: &[u8]) -> String {
    let mut hex = String::new();
    let mut ascii = String::new();
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 {
            hex.push(' ');
        }
        hex.push_str(&format!("{b:02x}"));
        ascii.push(if b.is_ascii_graphic() || *b == b' ' {
            *b as char
        } else {
            '.'
        });
    }
    format!("{hex}  |{ascii}|")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("milkyapps-pagemgr-{label}-{nanos}-{n}.db"))
    }

    struct TempFile {
        path: PathBuf,
    }

    impl TempFile {
        fn new(label: &str) -> Self {
            let path = temp_path(label);
            let _ = fs::remove_file(&path);
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    /// `open` on a missing path must create the file, initialize metadata page 0,
    /// and leave a single-page store with the default page size.
    #[test]
    fn open_creates_initialized_file() {
        let tmp = TempFile::new("create");
        assert!(!tmp.path().exists());

        let mgr = PageManager::open(tmp.path()).expect("open should create");
        assert_eq!(mgr.page_size(), DEFAULT_PAGE_SIZE);
        assert_eq!(mgr.page_count(), 1);
        assert!(tmp.path().exists());
        assert_eq!(
            fs::metadata(tmp.path()).unwrap().len(),
            DEFAULT_PAGE_SIZE as u64
        );
        drop(mgr);
    }

    /// Reopening an existing store must accept the on-disk metadata (page size,
    /// grown page count) rather than treating the file as empty/corrupt.
    #[test]
    fn open_existing_reuses_metadata() {
        let tmp = TempFile::new("reopen");
        {
            let mut mgr = PageManager::open(tmp.path()).unwrap();
            let id = mgr.alloc::<u64>().unwrap();
            *mgr.get_mut::<u64>(id) = 0xDEAD_BEEF;
            mgr.flush_all().unwrap();
        }

        let mgr = PageManager::open(tmp.path()).unwrap();
        assert!(mgr.page_count() >= 2);
        // Re-allocate should reuse the size-class list; first fresh alloc gets
        // another slot. Read back by walking is awkward without storing AllocId;
        // instead verify metadata page_size survived.
        assert_eq!(mgr.page_size(), DEFAULT_PAGE_SIZE);
    }

    /// After `dealloc`, the next `alloc` of the same size class must return the
    /// same `(page, slot)` — i.e. freed slots are recycled, not abandoned.
    #[test]
    fn alloc_dealloc_roundtrip_reuses_slot() {
        let tmp = TempFile::new("roundtrip");
        let mut mgr = PageManager::open(tmp.path()).unwrap();

        let a = mgr.alloc::<u32>().unwrap();
        *mgr.get_mut::<u32>(a) = 42;
        assert_eq!(*mgr.get_mut::<u32>(a), 42);

        mgr.dealloc(a);

        let b = mgr.alloc::<u32>().unwrap();
        assert_eq!(
            a, b,
            "after dealloc, the same page/slot should be handed out again"
        );
    }

    /// Values written into a slot must still be readable after `flush_all` and a
    /// full close/reopen of the mmap file (durability of payload bytes).
    #[test]
    fn alloc_writes_survive_flush_and_reopen() {
        let tmp = TempFile::new("persist");
        let (page, slot, size) = {
            let mut mgr = PageManager::open(tmp.path()).unwrap();
            let id = mgr.alloc::<u64>().unwrap();
            *mgr.get_mut::<u64>(id) = 0x1234_5678_9ABC_DEF0;
            mgr.flush_all().unwrap();
            (id.page_id(), id.slot_id(), id.size())
        };

        let mgr = PageManager::open(tmp.path()).unwrap();
        let id = AllocId {
            size,
            page_id: page,
            slot_id: slot,
        };
        let value = unsafe { *mgr.slot_ptr::<u64>(id) };
        assert_eq!(value, 0x1234_5678_9ABC_DEF0);
    }

    /// Objects that fall into different power-of-two size classes must be placed
    /// on different data pages (size-class freelists must not share pages).
    #[test]
    fn distinct_size_classes_use_distinct_pages() {
        let tmp = TempFile::new("classes");
        let mut mgr = PageManager::open(tmp.path()).unwrap();

        let small = mgr.alloc::<u8>().unwrap();
        let large = mgr.alloc::<[u8; 64]>().unwrap();

        assert_ne!(
            small.page_id(),
            large.page_id(),
            "different size classes must not share a data page"
        );
        assert_ne!(small.size(), large.size());
    }

    /// Once every slot on the current data page is taken, the next `alloc` must
    /// grow the file, link a new page into the size-class list, and keep prior
    /// slot contents intact.
    #[test]
    fn filling_a_page_allocates_another() {
        let tmp = TempFile::new("fill");
        // Tiny pages so a u64 size-class fills quickly.
        let page_size = 512;
        let mut mgr = PageManager::open_with_options(tmp.path(), page_size, 64).unwrap();

        let slot_size = size_of::<u64>().next_power_of_two();
        let slots = mgr.slots_per_page(slot_size);
        assert!(slots >= 1);

        let mut ids = Vec::with_capacity(slots + 1);
        for i in 0..=slots {
            let id = mgr.alloc::<u64>().expect("alloc");
            *mgr.get_mut::<u64>(id) = i as u64;
            ids.push(id);
        }

        let pages: std::collections::BTreeSet<_> = ids.iter().map(|id| id.page_id().0).collect();
        assert!(
            pages.len() >= 2,
            "expected at least two data pages after overflowing one, got {pages:?}"
        );

        for (i, id) in ids.iter().enumerate() {
            assert_eq!(*mgr.get_mut::<u64>(*id), i as u64);
        }
    }

    /// Repeated dealloc/alloc of the same logical slot must not append new pages
    /// to the file — recycling must not leak storage.
    #[test]
    fn dealloc_then_alloc_many_does_not_leak_pages() {
        let tmp = TempFile::new("noleak");
        let mut mgr = PageManager::open(tmp.path()).unwrap();

        let first = mgr.alloc::<u64>().unwrap();
        let pages_after_first = mgr.page_count();

        for _ in 0..64 {
            mgr.dealloc(first);
            let again = mgr.alloc::<u64>().unwrap();
            assert_eq!(again, first);
        }

        assert_eq!(
            mgr.page_count(),
            pages_after_first,
            "recycling must not grow the file"
        );
    }

    /// With a low `dirties_before_flush`, alloc/dealloc traffic must flush often
    /// enough that the in-memory dirty set stays bounded, and `flush_all` clears it.
    #[test]
    fn dirty_threshold_triggers_flush() {
        let tmp = TempFile::new("thresh");
        let mut mgr = PageManager::open_with_options(tmp.path(), DEFAULT_PAGE_SIZE, 2).unwrap();

        let a = mgr.alloc::<u64>().unwrap();
        // First alloc dirties data page (+ maybe meta). Threshold is 2, so a
        // flush may already have run; dirty set should be small either way.
        assert!(mgr.dirty.len() < 8);

        let b = mgr.alloc::<u32>().unwrap();
        let c = mgr.alloc::<u16>().unwrap();
        // After several size-class allocs, threshold flushes should have kept
        // the dirty list from growing without bound.
        assert!(
            mgr.dirty.len() < 16,
            "dirty list should be flushed periodically, len={}",
            mgr.dirty.len()
        );

        mgr.dealloc(a);
        mgr.dealloc(b);
        mgr.dealloc(c);
        mgr.flush_all().unwrap();
        assert!(mgr.dirty.is_empty());
    }

    /// `flush_page` must sync that page and remove it from the dirty set while
    /// leaving other bookkeeping usable.
    #[test]
    fn flush_page_removes_from_dirty_set() {
        let tmp = TempFile::new("flushpage");
        let mut mgr = PageManager::open_with_options(tmp.path(), DEFAULT_PAGE_SIZE, 100).unwrap();

        let id = mgr.alloc::<u64>().unwrap();
        assert!(mgr.dirty.contains(&id.page_id()));
        mgr.flush_page(id.page_id()).unwrap();
        assert!(!mgr.dirty.contains(&id.page_id()));
    }

    /// `alloc` must return `None` when the type cannot fit in a data page after
    /// the header (no silent truncation or overlapping slots).
    #[test]
    fn alloc_rejects_type_larger_than_page_payload() {
        let tmp = TempFile::new("toolarge");
        let mut mgr = PageManager::open(tmp.path()).unwrap();
        // An object the size of a full page cannot fit after the DataPage header.
        assert!(mgr.alloc::<[u8; DEFAULT_PAGE_SIZE]>().is_none());
    }

    /// Opening a file that is too small to hold a metadata page must fail
    /// cleanly (`None`) instead of mapping garbage.
    #[test]
    fn open_rejects_truncated_file() {
        let tmp = TempFile::new("trunc");
        fs::write(tmp.path(), [0u8; 16]).unwrap();
        let err = PageManager::open(tmp.path());
        assert!(err.is_none());
    }

    /// Two size classes must not interfere: freeing all slots of one class must
    /// not hand out pages/slots that still belong to another class.
    #[test]
    fn concurrent_size_class_lists_are_independent() {
        let tmp = TempFile::new("indep");
        let mut mgr = PageManager::open(tmp.path()).unwrap();

        let mut eights = vec![];
        let mut sixteens = vec![];
        for _ in 0..8 {
            eights.push(mgr.alloc::<u64>().unwrap());
            sixteens.push(mgr.alloc::<[u8; 16]>().unwrap());
        }

        for id in &eights {
            assert_eq!(id.size(), 8);
        }
        for id in &sixteens {
            assert_eq!(id.size(), 16);
        }

        // Free all eights; sixteens must remain allocated (bitmap untouched).
        for id in eights {
            mgr.dealloc(id);
        }
        for id in sixteens {
            // Still "used" — reallocating u64 must not return a 16-byte slot.
            let recycled = mgr.alloc::<u64>().unwrap();
            assert_eq!(recycled.size(), 8);
            assert_ne!(recycled.page_id(), id.page_id());
            mgr.dealloc(recycled);
        }
    }

    /// `page_layout` must pack as many slots as fit given a variable bitmap,
    /// and reject classes that cannot place even one slot.
    #[test]
    fn page_layout_derives_slot_count_from_page_size_and_class() {
        let layout = page_layout(512, size_class(64)).unwrap();
        assert_eq!(layout.slot_size, 64);
        assert_eq!(layout.slot_count, 7);
        assert_eq!(layout.bitmap_bytes, 1);
        assert_eq!(layout.payload_off, 64);

        let tiny = page_layout(512, size_class(1)).unwrap();
        assert_eq!(tiny.slot_count, 433);
        assert_eq!(tiny.bitmap_bytes, tiny.slot_count.div_ceil(8));

        assert!(page_layout(512, size_class(512)).is_none());
    }

    /// Unit-test the bitmap helpers in isolation: set/clear/test bits across
    /// byte boundaries without going through the page manager.
    #[test]
    fn bitmap_set_clear_helpers() {
        let mut bits = [0u8; 128];
        assert!(!bit_is_set(&bits, 0));
        set_bit(&mut bits, 0);
        assert!(bit_is_set(&bits, 0));
        set_bit(&mut bits, 7);
        set_bit(&mut bits, 8);
        assert!(bit_is_set(&bits, 7));
        assert!(bit_is_set(&bits, 8));
        clear_bit(&mut bits, 7);
        assert!(!bit_is_set(&bits, 7));
        assert!(bit_is_set(&bits, 8));
    }

    /// `size_class` must map each power-of-two slot size to its exponent
    /// (`2^i` → class `i`), which indexes `free_list_by_size`.
    #[test]
    fn size_class_matches_power_of_two_exponent() {
        assert_eq!(size_class(1), 0);
        assert_eq!(size_class(2), 1);
        assert_eq!(size_class(4), 2);
        assert_eq!(size_class(8), 3);
        assert_eq!(size_class(256), 8);
    }

    /// Dropping a `PageManager` must flush without panicking, and the file must
    /// remain openable afterward.
    #[test]
    fn drop_flushes_without_panic() {
        let tmp = TempFile::new("dropflush");
        let mut mgr = PageManager::open(tmp.path()).unwrap();
        let id = mgr.alloc::<u64>().unwrap();
        *mgr.get_mut::<u64>(id) = 7;
        drop(mgr);
        // File should still be readable / reopenable.
        let mgr = PageManager::open(tmp.path()).unwrap();
        assert_eq!(mgr.page_size(), DEFAULT_PAGE_SIZE);
    }

    /// Many successive allocations of the same type must yield unique
    /// `(page, slot)` pairs, and each slot must retain the value written to it
    /// (no double-hand-out / aliasing of live AllocIds).
    #[test]
    fn many_allocations_are_distinct() {
        let tmp = TempFile::new("distinct");
        let mut mgr = PageManager::open(tmp.path()).unwrap();

        let mut ids = Vec::new();
        for i in 0..128 {
            let id = mgr.alloc::<u64>().unwrap();
            *mgr.get_mut::<u64>(id) = i;
            ids.push(id);
        }

        let mut seen = ids.clone();
        seen.sort_by_key(|a| (a.page_id().0, a.slot_id().0));
        seen.dedup();
        assert_eq!(seen.len(), ids.len(), "duplicate AllocIds handed out");

        for (i, id) in ids.iter().enumerate() {
            assert_eq!(*mgr.get_mut::<u64>(*id), i as u64);
        }
    }
}
