//! [`Storage`] façade: WAL + page manager + ART index.

use std::io;
use std::path::{Path, PathBuf};

use crate::sim::Sim;
use crate::storage::arttrie::{ArtTrie, GetResult};
use crate::storage::pagemgr::{AllocId, PageManager};
use crate::storage::wal::{
    AppendConfig, FlushStrategy, FsApi, RustStdApi, Wal, WalError, WalEvent, WalOptions,
};
use crate::sync::bounded::Receiver;

/// Consumer cursor persisted in page-0 custom metadata.
///
/// `wal_offset` is the next WAL byte offset that still needs to be applied to
/// the index for recovery. The live write path advances this as soon as a
/// record has been appended and applied to the ART (without waiting for a WAL
/// flush).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StorageConsumer {
    /// Next WAL offset to process on recovery / catch-up.
    pub wal_offset: u64,
}

/// On-disk layout in page-0 custom data.
///
/// ```text
/// [wal_offset:u64][root_size:u64][root_page:u64][root_slot:u64]
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct OnDiskMeta {
    wal_offset: u64,
    root_size: u64,
    root_page: u64,
    root_slot: u64,
}

impl OnDiskMeta {
    const BYTE_LEN: usize = 32;

    fn to_bytes(self) -> [u8; Self::BYTE_LEN] {
        let mut out = [0u8; Self::BYTE_LEN];
        out[0..8].copy_from_slice(&self.wal_offset.to_le_bytes());
        out[8..16].copy_from_slice(&self.root_size.to_le_bytes());
        out[16..24].copy_from_slice(&self.root_page.to_le_bytes());
        out[24..32].copy_from_slice(&self.root_slot.to_le_bytes());
        out
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let mut buf = [0u8; Self::BYTE_LEN];
        let n = bytes.len().min(Self::BYTE_LEN);
        buf[..n].copy_from_slice(&bytes[..n]);
        Self {
            wal_offset: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            root_size: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            root_page: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            root_slot: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
        }
    }

    fn root(self) -> Option<AllocId> {
        if self.root_size == 0 {
            None
        } else {
            Some(AllocId::from_raw(
                self.root_size as usize,
                self.root_page as usize,
                self.root_slot as usize,
            ))
        }
    }

    fn with_root(mut self, root: Option<AllocId>) -> Self {
        match root {
            None => {
                self.root_size = 0;
                self.root_page = 0;
                self.root_slot = 0;
            }
            Some(id) => {
                self.root_size = id.size() as u64;
                self.root_page = id.page_id().index() as u64;
                self.root_slot = id.slot_id().index() as u64;
            }
        }
        self
    }
}

/// Options for opening a [`Storage`] instance.
#[derive(Debug, Clone)]
pub struct StorageOptions {
    /// Directory for the page-manager file and WAL segment.
    pub dir: PathBuf,
    /// Page-manager file name under [`Self::dir`].
    pub pages_file: String,
    /// WAL segment file name under [`Self::dir`].
    pub wal_file: String,
    /// Logical page size when creating a new page file.
    pub page_size: usize,
    /// Dirty-page flush threshold for the page manager.
    pub dirties_before_flush: usize,
    /// WAL options (preallocate, max size, notify capacity, …).
    pub wal: WalOptions,
}

impl StorageOptions {
    /// Defaults under `dir` (`pages.db`, `wal.log`, 64 KiB pages).
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        let wal = WalOptions::new(&dir, "wal.log");
        Self {
            dir,
            pages_file: "pages.db".into(),
            wal_file: "wal.log".into(),
            page_size: 65536,
            dirties_before_flush: 16,
            wal,
        }
    }
}

/// Errors from [`Storage`] operations.
#[derive(Debug)]
pub enum StorageError {
    /// Page-manager / metadata I/O.
    Io(io::Error),
    /// WAL failure.
    Wal(WalError),
}

impl From<io::Error> for StorageError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<WalError> for StorageError {
    fn from(value: WalError) -> Self {
        Self::Wal(value)
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "storage i/o: {e}"),
            Self::Wal(e) => write!(f, "storage wal: {e}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Wal(e) => Some(e),
        }
    }
}

/// Composed storage engine: owns the WAL, page manager, and ART index.
pub struct Storage<A: FsApi = RustStdApi> {
    wal: Wal<A>,
    pages: PageManager,
    index: ArtTrie,
    /// Notification receiver for WAL appends (caller may take via [`Self::take_wal_notify`]).
    wal_notify: Option<Receiver<WalEvent>>,
    /// Page-file path (real path or sim key).
    pages_path: PathBuf,
}

impl Storage<RustStdApi> {
    /// Open or create storage under `options` (OS filesystem + mmap pages).
    ///
    /// # Errors
    ///
    /// Returns I/O or WAL errors if files cannot be prepared.
    pub fn open(options: StorageOptions) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&options.dir)?;
        let pages_path = options.dir.join(&options.pages_file);
        let pages = PageManager::open_with_options(
            &pages_path,
            options.page_size,
            options.dirties_before_flush,
        )?;
        Self::from_parts(options, pages, pages_path, RustStdApi)
    }

    /// Directory-relative helper: open with [`StorageOptions::new`].
    ///
    /// # Errors
    ///
    /// Returns I/O or WAL errors if files cannot be prepared.
    pub fn open_in(dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open(StorageOptions::new(dir.as_ref()))
    }
}

impl Storage<crate::sim::SimFs> {
    /// Open storage inside a [`Sim`] world (simulated WAL FS + memory pages).
    ///
    /// Page durability is tracked by [`PageManager`]'s memory backend; before
    /// [`Sim::crash`], call [`Self::sync_sim_pages`] so the sim retains the
    /// durable page image.
    ///
    /// # Errors
    ///
    /// Returns I/O or WAL errors if the sim backends cannot be prepared.
    pub fn open_sim(sim: &Sim, options: StorageOptions) -> Result<Self, StorageError> {
        let pages_path = options.dir.join(&options.pages_file);
        let durable = sim.pages_durable(&pages_path).unwrap_or_default();
        let pages =
            PageManager::open_memory(&durable, options.page_size, options.dirties_before_flush)?;
        sim.log(format!(
            "storage_open_sim(pages={}, wal={})",
            pages_path.display(),
            options.wal_file
        ));
        Self::from_parts(options, pages, pages_path, sim.fs())
    }

    /// Flush dirty pages, then publish the durable image into `sim`.
    ///
    /// Call before [`Sim::crash`] / reopen so the sim retains flushed page
    /// bytes (WAL durability remains separate via [`SimFs`] sync).
    ///
    /// # Errors
    ///
    /// Returns page-manager flush errors.
    pub fn sync_sim_pages(&mut self, sim: &Sim) -> Result<(), StorageError> {
        debug_assert!(self.pages.is_memory_backed());
        self.pages.flush_all()?;
        sim.install_pages(&self.pages_path, self.pages.durable_image());
        Ok(())
    }
}

impl<A: FsApi> Storage<A> {
    fn from_parts(
        options: StorageOptions,
        pages: PageManager,
        pages_path: PathBuf,
        fs: A,
    ) -> Result<Self, StorageError> {
        let mut wal_opts = options.wal;
        wal_opts.dir = options.dir;
        wal_opts.file_name = options.wal_file;
        let (wal, wal_notify) = Wal::with_options_and_fs(wal_opts, fs)?;
        let meta = OnDiskMeta::from_bytes(pages.get_custom_data());
        let mut index = ArtTrie::new();
        index.set_root(meta.root());
        Ok(Self {
            wal,
            pages,
            index,
            wal_notify: Some(wal_notify),
            pages_path,
        })
    }

    /// Take the WAL notification receiver (once).
    pub fn take_wal_notify(&mut self) -> Option<Receiver<WalEvent>> {
        self.wal_notify.take()
    }

    /// Borrow the WAL.
    #[must_use]
    pub const fn wal(&self) -> &Wal<A> {
        &self.wal
    }

    /// Borrow the page manager.
    #[must_use]
    pub const fn pages(&self) -> &PageManager {
        &self.pages
    }

    /// Borrow the ART index.
    #[must_use]
    pub const fn index(&self) -> &ArtTrie {
        &self.index
    }

    /// Page-file path used by this instance.
    #[must_use]
    pub fn pages_path(&self) -> &Path {
        &self.pages_path
    }

    /// Read [`StorageConsumer`] from page-0 custom metadata.
    #[must_use]
    pub fn consumer(&self) -> StorageConsumer {
        StorageConsumer {
            wal_offset: OnDiskMeta::from_bytes(self.pages.get_custom_data()).wal_offset,
        }
    }

    /// Persist [`StorageConsumer`] and the live ART root into page-0 custom metadata.
    ///
    /// # Errors
    ///
    /// Returns I/O errors if the custom region cannot be written.
    pub fn set_consumer(&mut self, consumer: StorageConsumer) -> Result<(), StorageError> {
        let meta = OnDiskMeta {
            wal_offset: consumer.wal_offset,
            ..OnDiskMeta::from_bytes(self.pages.get_custom_data())
        }
        .with_root(self.index.root());
        self.pages.set_custom_data(&meta.to_bytes())?;
        Ok(())
    }

    fn persist_live_meta(&mut self, wal_offset: u64) -> Result<(), StorageError> {
        let meta = OnDiskMeta {
            wal_offset,
            ..OnDiskMeta::default()
        }
        .with_root(self.index.root());
        self.pages.set_custom_data(&meta.to_bytes())?;
        Ok(())
    }

    /// Append `key`/`value` to the WAL and apply them to the ART immediately
    /// (does not wait for a WAL flush).
    ///
    /// Advances [`StorageConsumer::wal_offset`] to the end of the new record.
    ///
    /// Record payload format: `[key_len:u32 LE][key][value]`.
    ///
    /// # Errors
    ///
    /// Returns WAL or metadata I/O errors.
    pub fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        append_cfg: AppendConfig,
    ) -> Result<WalEvent, StorageError> {
        let key_len = u32::try_from(key.len()).map_err(|_| {
            StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "key longer than u32::MAX",
            ))
        })?;
        let mut record = Vec::with_capacity(4 + key.len() + value.len());
        record.extend_from_slice(&key_len.to_le_bytes());
        record.extend_from_slice(key);
        record.extend_from_slice(value);

        let event = self.wal.append(&record, append_cfg)?;
        self.index.insert(&mut self.pages, key, value);

        let next = event
            .offset
            .checked_add(4)
            .and_then(|o| o.checked_add(u64::from(event.payload_len)))
            .ok_or_else(|| {
                StorageError::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "wal offset overflow",
                ))
            })?;
        self.persist_live_meta(next)?;
        Ok(event)
    }

    /// Look up `key` in the live ART index.
    pub fn get<'a>(&'a mut self, key: &[u8]) -> GetResult<'a> {
        self.index.get(&mut self.pages, key)
    }

    /// Flush the WAL with `strategy` (does not publish a durable trie snapshot).
    ///
    /// # Errors
    ///
    /// Returns WAL flush errors.
    pub fn flush_wal(&self, strategy: FlushStrategy) -> Result<(), StorageError> {
        Ok(self.wal.flush(strategy)?)
    }

    /// Flush dirty pages (and optionally all pages).
    ///
    /// # Errors
    ///
    /// Returns page-manager I/O errors.
    pub fn flush_pages(&mut self, all: bool) -> Result<(), StorageError> {
        if all {
            self.pages.flush_all()?;
        } else {
            self.pages.flush_dirty()?;
        }
        Ok(())
    }

    /// Current live ART root handle, if any.
    #[must_use]
    pub const fn live_root(&self) -> Option<AllocId> {
        self.index.root()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::AppendConfig;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir(label: &str) -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("milkyapps-storage-{label}-{n}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn put_get_and_reopen() {
        let dir = tmp_dir("roundtrip");
        {
            let mut s = Storage::open_in(&dir).unwrap();
            s.put(b"a", b"1", AppendConfig::flush_data()).unwrap();
            s.put(b"b", b"2", AppendConfig::no_flush()).unwrap();
            assert_eq!(s.get(b"a").value, Some(b"1".as_slice()));
            assert_eq!(s.get(b"b").value, Some(b"2".as_slice()));
            assert!(s.consumer().wal_offset > 0);
            s.flush_pages(true).unwrap();
        }
        let mut s = Storage::open_in(&dir).unwrap();
        assert_eq!(s.get(b"a").value, Some(b"1".as_slice()));
        assert_eq!(s.get(b"b").value, Some(b"2".as_slice()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn custom_data_roundtrip_via_pages() {
        let dir = tmp_dir("custom");
        let pages_path = dir.join("pages.db");
        {
            let mut pages = PageManager::open_with_options(&pages_path, 4096, 4).unwrap();
            pages.set_custom_data(b"hello-meta").unwrap();
            pages.flush_all().unwrap();
        }
        let pages = PageManager::open_with_options(&pages_path, 4096, 4).unwrap();
        assert!(pages.get_custom_data().starts_with(b"hello-meta"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn sim_live_meta_reopen_finds_key() {
        use crate::sim::Sim;
        use crate::storage::wal::AppendConfig;
        let sim = Sim::new(4);
        let dir = std::path::PathBuf::from("/sim/unit_live");
        let mut opts = StorageOptions::new(&dir);
        opts.page_size = 4096;
        opts.dirties_before_flush = 1;
        opts.wal = opts.wal.clone().without_preallocate().max_file_bytes(0).notify_capacity(8);
        {
            let mut s = Storage::open_sim(&sim, opts.clone()).unwrap();
            s.put(b"ghost", b"1", AppendConfig::no_flush()).unwrap();
            assert!(s.get(b"ghost").value.is_some(), "before crash");
            s.sync_sim_pages(&sim).unwrap();
        }
        sim.crash();
        let mut s = Storage::open_sim(&sim, opts).unwrap();
        assert_eq!(s.get(b"ghost").value, Some(b"1".as_slice()), "after crash reopen");
    }
}
