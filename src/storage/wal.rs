//! Append-only write-ahead log of opaque byte blocks.
//!
//! # On-disk layout
//!
//! Each record is framed as little-endian length followed by payload:
//!
//! ```text
//! [len: u32 LE][payload: len bytes]
//! ```
//!
//! `len == 0` is reserved and means “unused / preallocated space”. Empty
//! payloads are therefore rejected by [`Wal::append`]. On open, the WAL scans
//! until it hits a zero length, an incomplete header/payload, or EOF, then
//! continues appending from that logical end.
//!
//! # Filesystem consistency considerations
//!
//! Drawn from [Files are hard](https://danluu.com/file-consistency/) (and the
//! Pillai et al. OSDI '14 work it summarizes). These drive the [`FsApi`]
//! abstraction and the flush options on [`AppendConfig`]:
//!
//! 1. **Multi-byte writes are not atomic.** A crash mid-`write` can tear a
//!    record. Framing + scan-on-open truncates an incomplete trailing record
//!    instead of treating garbage as valid data.
//! 2. **Syscall order is not guaranteed without barriers.** Durability and
//!    ordering require explicit flush (`fdatasync` / `fsync`). See
//!    [`FlushStrategy`].
//! 3. **`fsync` on a file does not sync the directory entry.** Creating or
//!    renaming a WAL segment must also sync the parent directory
//!    ([`FsApi::sync_dir`]), or a crash can lose the directory entry even
//!    though file data was synced.
//! 4. **Metadata vs data ordering varies by FS** (`data=ordered` /
//!    `writeback` / `journal`). Never assume a write to file A is on disk
//!    before a later write to file B without flushes in between.
//! 5. **Extending a file can expose stale or zero-filled bytes** if size is
//!    updated before data lands (`data=writeback`). We only treat `len >= 1`
//!    records as valid; trailing preallocated zeros read as `len == 0` and
//!    end the scan.
//! 6. **`fsync` is not always a device flush.** Some OS / FS configs delay or
//!    elide flushes (historically OS X without `F_FULLFSYNC`, some ext3
//!    setups). [`RustStdApi`] uses `File::sync_data` / `sync_all`; a future
//!    platform-specific [`FsApi`] can issue stronger barriers.
//! 7. **Disks can silently corrupt data.** This WAL stores opaque bytes and
//!    does not checksum payloads (callers that need it should include a
//!    checksum inside `block`). Corruption should ideally destroy one record,
//!    not the whole log — the length prefix limits blast radius if lengths
//!    stay intact.
//! 8. **Preallocate capacity** to avoid repeated metadata updates while
//!    appending. [`WalOptions::preallocate_bytes`] extends the file with
//!    zeros ahead of the write cursor; those zeros are not valid records.
//! 9. **Prefer libraries over ad-hoc syscalls.** The [`FsApi`] trait is the
//!    seam for stronger backends (e.g. ext4 `fallocate`, `F_FULLFSYNC`)
//!    without rewriting WAL logic.
//!
//! # Notifications
//!
//! After a successful append (and any requested flush), the WAL sends a
//! [`WalEvent`] on a bounded channel from this crate so consumers can wake
//! and make progress.

use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::sync::bounded::{self, Receiver, Sender};

/// Size of the per-record length prefix in bytes.
pub const LENGTH_PREFIX_LEN: usize = 4;

/// Default bytes reserved ahead of the write cursor when preallocation is enabled.
pub const DEFAULT_PREALLOCATE_BYTES: u64 = 64 * 1024 * 1024;

/// Default maximum size of a single WAL segment before rolling is considered.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 256 * 1024 * 1024;

/// Default capacity of the notification channel.
pub const DEFAULT_NOTIFY_CAPACITY: usize = 1024;

/// How hard the WAL should try to push appended bytes to stable storage.
///
/// Stronger strategies are slower; pick per-`append` via [`AppendConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FlushStrategy {
    /// Do not flush. Fastest; a crash may lose recent appends still in page cache.
    #[default]
    None,
    /// Flush file data (`fdatasync` / [`File::sync_data`]). Does not guarantee
    /// metadata (e.g. size) is stable on every filesystem.
    Data,
    /// Flush data and metadata (`fsync` / [`File::sync_all`]).
    All,
}

/// Per-append knobs (flush strategy and similar).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AppendConfig {
    /// Durability barrier to take after the write completes.
    pub flush: FlushStrategy,
}

impl AppendConfig {
    /// Append without an explicit flush.
    #[must_use]
    pub const fn no_flush() -> Self {
        Self {
            flush: FlushStrategy::None,
        }
    }

    /// Append and flush file data afterward.
    #[must_use]
    pub const fn flush_data() -> Self {
        Self {
            flush: FlushStrategy::Data,
        }
    }

    /// Append and fully sync the file afterward.
    #[must_use]
    pub const fn flush_all() -> Self {
        Self {
            flush: FlushStrategy::All,
        }
    }
}

/// Long-lived WAL configuration (directory, rolling, preallocation, …).
#[derive(Debug, Clone)]
pub struct WalOptions {
    /// Directory that holds WAL segment files.
    pub dir: PathBuf,
    /// File name of the active segment (created under [`Self::dir`]).
    pub file_name: String,
    /// When the cursor would pass this many bytes past the current file
    /// length, extend the file by this many bytes (zeros). `0` disables.
    pub preallocate_bytes: u64,
    /// Soft maximum segment size. When an append would exceed this, the WAL
    /// errors with [`WalError::SegmentFull`] so the caller can roll segments.
    /// `0` means unlimited.
    pub max_file_bytes: u64,
    /// Capacity of the [`WalEvent`] notification channel.
    pub notify_capacity: usize,
}

impl WalOptions {
    /// Options targeting `dir` / `file_name` with performance-oriented defaults.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>, file_name: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            file_name: file_name.into(),
            preallocate_bytes: DEFAULT_PREALLOCATE_BYTES,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            notify_capacity: DEFAULT_NOTIFY_CAPACITY,
        }
    }

    /// Disable preallocation.
    #[must_use]
    pub fn without_preallocate(mut self) -> Self {
        self.preallocate_bytes = 0;
        self
    }

    /// Set preallocation chunk size in bytes (`0` disables).
    #[must_use]
    pub fn preallocate_bytes(mut self, bytes: u64) -> Self {
        self.preallocate_bytes = bytes;
        self
    }

    /// Set soft maximum segment size (`0` = unlimited).
    #[must_use]
    pub fn max_file_bytes(mut self, bytes: u64) -> Self {
        self.max_file_bytes = bytes;
        self
    }

    /// Set notification channel capacity.
    #[must_use]
    pub fn notify_capacity(mut self, capacity: usize) -> Self {
        self.notify_capacity = capacity;
        self
    }
}

/// Notification emitted after a durable-enough append (per [`AppendConfig`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalEvent {
    /// Byte offset of the record’s length prefix within the segment.
    pub offset: u64,
    /// Payload length in bytes (excludes the 4-byte length prefix).
    pub payload_len: u32,
}

/// Errors produced by WAL operations.
#[derive(Debug)]
pub enum WalError {
    /// Underlying I/O failure.
    Io(io::Error),
    /// Payload was empty (`len == 0` is reserved for unused space).
    EmptyPayload,
    /// Payload longer than `u32::MAX`.
    PayloadTooLarge,
    /// Append would exceed [`WalOptions::max_file_bytes`].
    SegmentFull {
        /// Current logical end.
        logical_len: u64,
        /// Configured maximum.
        max_file_bytes: u64,
    },
    /// Notification channel was closed (all receivers dropped).
    NotifyClosed,
}

impl From<io::Error> for WalError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "wal i/o error: {e}"),
            Self::EmptyPayload => write!(f, "wal rejects empty payloads"),
            Self::PayloadTooLarge => write!(f, "wal payload exceeds u32::MAX"),
            Self::SegmentFull {
                logical_len,
                max_file_bytes,
            } => write!(
                f,
                "wal segment full: logical_len={logical_len} max={max_file_bytes}"
            ),
            Self::NotifyClosed => write!(f, "wal notify channel closed"),
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// Filesystem operations the WAL needs, so backends can specialize (ext4,
/// `F_FULLFSYNC`, …) without changing append logic.
pub trait FsApi {
    /// File handle type used by this backend.
    type File;

    /// Create `dir` and parents if missing.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying filesystem.
    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;

    /// Returns whether `path` exists.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying filesystem when existence cannot
    /// be determined.
    fn exists(&self, path: &Path) -> io::Result<bool>;

    /// Open (or create) a read/write segment file for appending.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying filesystem.
    fn open_segment(&self, path: &Path) -> io::Result<Self::File>;

    /// Sync the directory so newly created names survive a crash.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying filesystem.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;

    /// Current length of `file` as reported by metadata.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying filesystem.
    fn len(&self, file: &Self::File) -> io::Result<u64>;

    /// Grow or shrink `file` to exactly `len` bytes.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying filesystem.
    fn set_len(&self, file: &Self::File, len: u64) -> io::Result<()>;

    /// Seek within `file`.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying filesystem.
    fn seek(&self, file: &mut Self::File, pos: SeekFrom) -> io::Result<u64>;

    /// Read into `buf`, returning bytes read (0 = EOF).
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying filesystem.
    fn read(&self, file: &mut Self::File, buf: &mut [u8]) -> io::Result<usize>;

    /// Write the full contents of `bufs` in order (like `write_all` + vectored).
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying filesystem.
    fn write_all_vectored(&self, file: &mut Self::File, bufs: &mut [IoSlice<'_>])
    -> io::Result<()>;

    /// Flush file data to stable storage (`fdatasync` / [`File::sync_data`]).
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying filesystem.
    fn sync_data(&self, file: &Self::File) -> io::Result<()>;

    /// Flush file data and metadata (`fsync` / [`File::sync_all`]).
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the underlying filesystem.
    fn sync_all(&self, file: &Self::File) -> io::Result<()>;
}

/// [`FsApi`] implemented with the Rust standard library (`std::fs` / `std::io`).
#[derive(Debug, Default, Clone, Copy)]
pub struct RustStdApi;

impl FsApi for RustStdApi {
    type File = File;

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dir)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
    }

    fn open_segment(&self, path: &Path) -> io::Result<Self::File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        // On most Unix platforms, opening the directory and syncing it persists
        // directory entries. On platforms without directory handles this is a
        // best-effort no-op after verifying the path exists.
        let meta = std::fs::metadata(dir)?;
        if !meta.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "sync_dir path is not a directory",
            ));
        }

        #[cfg(unix)]
        {
            let dir_file = OpenOptions::new().read(true).open(dir)?;
            dir_file.sync_all()?;
        }

        #[cfg(not(unix))]
        {
            let _ = dir;
        }

        Ok(())
    }

    fn len(&self, file: &Self::File) -> io::Result<u64> {
        Ok(file.metadata()?.len())
    }

    fn set_len(&self, file: &Self::File, len: u64) -> io::Result<()> {
        file.set_len(len)
    }

    fn seek(&self, file: &mut Self::File, pos: SeekFrom) -> io::Result<u64> {
        file.seek(pos)
    }

    fn read(&self, file: &mut Self::File, buf: &mut [u8]) -> io::Result<usize> {
        file.read(buf)
    }

    fn write_all_vectored(
        &self,
        file: &mut Self::File,
        bufs: &mut [IoSlice<'_>],
    ) -> io::Result<()> {
        write_all_vectored(file, bufs)
    }

    fn sync_data(&self, file: &Self::File) -> io::Result<()> {
        file.sync_data()
    }

    fn sync_all(&self, file: &Self::File) -> io::Result<()> {
        file.sync_all()
    }
}

/// Write every byte in `bufs`, advancing through partial vectored writes.
fn write_all_vectored(file: &mut File, mut bufs: &mut [IoSlice<'_>]) -> io::Result<()> {
    while !bufs.is_empty() {
        match file.write_vectored(bufs) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            Ok(n) => IoSlice::advance_slices(&mut bufs, n),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Append-only write-ahead log of opaque byte blocks.
pub struct Wal<A: FsApi = RustStdApi> {
    fs: A,
    options: WalOptions,
    path: PathBuf,
    file: A::File,
    /// Offset of the next byte to write (end of last complete record).
    logical_len: u64,
    /// Physical size of the file (may be ahead of `logical_len` due to prealloc).
    file_len: u64,
    notify: Sender<WalEvent>,
}

impl Wal<RustStdApi> {
    /// Open or create a WAL under `options` using [`RustStdApi`].
    ///
    /// Returns the WAL and the receiving end of the notification channel.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] if the directory/file cannot be prepared or scanned.
    pub fn with_options(options: WalOptions) -> Result<(Self, Receiver<WalEvent>), WalError> {
        Self::with_options_and_fs(options, RustStdApi)
    }
}

impl<A: FsApi> Wal<A> {
    /// Open or create a WAL under `options` with a custom filesystem backend.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] if the directory/file cannot be prepared or scanned.
    pub fn with_options_and_fs(
        options: WalOptions,
        fs: A,
    ) -> Result<(Self, Receiver<WalEvent>), WalError> {
        if options.notify_capacity == 0 {
            return Err(WalError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "notify_capacity must be greater than zero",
            )));
        }

        fs.create_dir_all(&options.dir)?;
        let path = options.dir.join(&options.file_name);
        let created = !fs.exists(&path)?;
        let mut file = fs.open_segment(&path)?;
        if created {
            // Persist the directory entry for the new segment.
            fs.sync_dir(&options.dir)?;
        }

        let file_len = fs.len(&file)?;
        let logical_len = scan_logical_end(&fs, &mut file, file_len)?;
        if logical_len < file_len {
            // Drop a torn trailing record; keep any preallocated tail only if
            // it is pure unused space (zeros). We truncate to logical_len when
            // the remainder is a partial record (not aligned unused space).
            // Re-read: if the next 4 bytes are zero (or missing), keep capacity.
            fs.seek(&mut file, SeekFrom::Start(logical_len))?;
            let mut hdr = [0u8; LENGTH_PREFIX_LEN];
            let n = read_exact_or_eof(&fs, &mut file, &mut hdr)?;
            let keep_prealloc = n == 0 || (n == LENGTH_PREFIX_LEN && u32::from_le_bytes(hdr) == 0);
            if !keep_prealloc {
                fs.set_len(&file, logical_len)?;
            }
        }

        let file_len = fs.len(&file)?;
        fs.seek(&mut file, SeekFrom::Start(logical_len))?;

        let (notify, receiver) = bounded::bounded(options.notify_capacity);

        Ok((
            Self {
                fs,
                options,
                path,
                file,
                logical_len,
                file_len,
                notify,
            },
            receiver,
        ))
    }

    /// Path of the active segment file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Logical end offset (next append position).
    #[must_use]
    pub const fn logical_len(&self) -> u64 {
        self.logical_len
    }

    /// Append one opaque block using `cfg`’s flush strategy.
    ///
    /// Uses vectored I/O for the length prefix + payload to avoid allocating a
    /// combined buffer. May pre-extend the file when configured.
    ///
    /// # Errors
    ///
    /// - [`WalError::EmptyPayload`] if `block` is empty
    /// - [`WalError::PayloadTooLarge`] if `block` exceeds `u32::MAX`
    /// - [`WalError::SegmentFull`] if the append would exceed the configured max
    /// - [`WalError::NotifyClosed`] if all receivers were dropped
    /// - [`WalError::Io`] on filesystem failures
    pub fn append(&mut self, block: &[u8], cfg: AppendConfig) -> Result<WalEvent, WalError> {
        if block.is_empty() {
            return Err(WalError::EmptyPayload);
        }
        let payload_len = u32::try_from(block.len()).map_err(|_| WalError::PayloadTooLarge)?;
        let record_len = LENGTH_PREFIX_LEN as u64 + u64::from(payload_len);

        if self.options.max_file_bytes != 0
            && self
                .logical_len
                .checked_add(record_len)
                .is_none_or(|end| end > self.options.max_file_bytes)
        {
            return Err(WalError::SegmentFull {
                logical_len: self.logical_len,
                max_file_bytes: self.options.max_file_bytes,
            });
        }

        self.ensure_capacity(record_len)?;

        let offset = self.logical_len;
        let header = payload_len.to_le_bytes();
        let mut bufs = [IoSlice::new(&header), IoSlice::new(block)];
        self.fs.write_all_vectored(&mut self.file, &mut bufs)?;

        self.logical_len += record_len;

        match cfg.flush {
            FlushStrategy::None => {}
            FlushStrategy::Data => self.fs.sync_data(&self.file)?,
            FlushStrategy::All => self.fs.sync_all(&self.file)?,
        }

        let event = WalEvent {
            offset,
            payload_len,
        };
        self.notify
            .send(event)
            .map_err(|_| WalError::NotifyClosed)?;
        Ok(event)
    }

    /// Flush using `strategy` without appending.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] if the flush fails.
    pub fn flush(&self, strategy: FlushStrategy) -> Result<(), WalError> {
        match strategy {
            FlushStrategy::None => Ok(()),
            FlushStrategy::Data => Ok(self.fs.sync_data(&self.file)?),
            FlushStrategy::All => Ok(self.fs.sync_all(&self.file)?),
        }
    }

    /// Iterate complete records currently in the segment (replay / catch-up).
    ///
    /// Allocates one `Vec` per record. Prefer the notification stream for
    /// live consumers that already know offsets.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::Io`] if reading fails.
    pub fn records(&mut self) -> Result<Vec<(u64, Vec<u8>)>, WalError> {
        let mut out = Vec::new();
        self.fs.seek(&mut self.file, SeekFrom::Start(0))?;
        let mut pos = 0u64;
        while pos < self.logical_len {
            let mut hdr = [0u8; LENGTH_PREFIX_LEN];
            read_exact(&self.fs, &mut self.file, &mut hdr)?;
            let len = u32::from_le_bytes(hdr);
            if len == 0 {
                break;
            }
            let mut buf = vec![0u8; len as usize];
            read_exact(&self.fs, &mut self.file, &mut buf)?;
            out.push((pos, buf));
            pos += LENGTH_PREFIX_LEN as u64 + u64::from(len);
        }
        self.fs
            .seek(&mut self.file, SeekFrom::Start(self.logical_len))?;
        Ok(out)
    }

    fn ensure_capacity(&mut self, needed: u64) -> Result<(), WalError> {
        let end = self
            .logical_len
            .checked_add(needed)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "wal size overflow"))?;
        if end <= self.file_len {
            return Ok(());
        }

        let chunk = if self.options.preallocate_bytes == 0 {
            needed
        } else {
            let deficit = end - self.file_len;
            let chunks = deficit.div_ceil(self.options.preallocate_bytes);
            chunks.saturating_mul(self.options.preallocate_bytes)
        };

        let new_len = self
            .file_len
            .checked_add(chunk)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "wal size overflow"))?;
        self.fs.set_len(&self.file, new_len)?;
        self.file_len = new_len;
        // `set_len` may move the cursor on some platforms; re-seek.
        self.fs
            .seek(&mut self.file, SeekFrom::Start(self.logical_len))?;
        Ok(())
    }
}

fn scan_logical_end<A: FsApi>(fs: &A, file: &mut A::File, file_len: u64) -> io::Result<u64> {
    fs.seek(file, SeekFrom::Start(0))?;
    let mut pos = 0u64;
    let mut hdr = [0u8; LENGTH_PREFIX_LEN];

    while pos < file_len {
        let remaining = file_len - pos;
        if remaining < LENGTH_PREFIX_LEN as u64 {
            // Torn length prefix.
            return Ok(pos);
        }

        read_exact(fs, file, &mut hdr)?;
        let len = u32::from_le_bytes(hdr);
        if len == 0 {
            // Preallocated / unused space.
            return Ok(pos);
        }

        let payload = u64::from(len);
        if remaining < LENGTH_PREFIX_LEN as u64 + payload {
            // Torn payload.
            return Ok(pos);
        }

        pos += LENGTH_PREFIX_LEN as u64 + payload;
        fs.seek(file, SeekFrom::Start(pos))?;
    }

    Ok(pos)
}

fn read_exact<A: FsApi>(fs: &A, file: &mut A::File, buf: &mut [u8]) -> io::Result<()> {
    let mut offset = 0;
    while offset < buf.len() {
        match fs.read(file, &mut buf[offset..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            Ok(n) => offset += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn read_exact_or_eof<A: FsApi>(fs: &A, file: &mut A::File, buf: &mut [u8]) -> io::Result<usize> {
    let mut read = 0;
    while read < buf.len() {
        match fs.read(file, &mut buf[read..]) {
            Ok(0) => return Ok(read),
            Ok(n) => read += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let path = std::env::temp_dir().join(format!("milkyapps-wal-{label}-{nanos}"));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn opts(dir: &Path) -> WalOptions {
        WalOptions::new(dir, "segment.wal")
            .without_preallocate()
            .max_file_bytes(0)
            .notify_capacity(16)
    }

    #[test]
    fn append_and_replay() {
        let tmp = TempDir::new("replay");
        let (mut wal, rx) = Wal::with_options(opts(tmp.path())).unwrap();

        let e1 = wal.append(b"hello", AppendConfig::flush_data()).unwrap();
        let e2 = wal.append(b"world", AppendConfig::no_flush()).unwrap();
        wal.flush(FlushStrategy::All).unwrap();

        assert_eq!(e1.offset, 0);
        assert_eq!(e1.payload_len, 5);
        assert_eq!(e2.offset, 4 + 5);
        assert_eq!(rx.recv(), Some(e1));
        assert_eq!(rx.recv(), Some(e2));

        let records = wal.records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].1, b"hello");
        assert_eq!(records[1].1, b"world");
    }

    #[test]
    fn reopen_continues_existing_file() {
        let tmp = TempDir::new("reopen");
        {
            let (mut wal, _rx) = Wal::with_options(opts(tmp.path())).unwrap();
            wal.append(b"one", AppendConfig::flush_all()).unwrap();
            wal.append(b"two", AppendConfig::flush_all()).unwrap();
        }
        let (mut wal, _rx) = Wal::with_options(opts(tmp.path())).unwrap();
        assert_eq!(wal.logical_len(), (4 + 3) + (4 + 3));
        wal.append(b"three", AppendConfig::flush_all()).unwrap();
        let records = wal.records().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[2].1, b"three");
    }

    #[test]
    fn rejects_empty_payload() {
        let tmp = TempDir::new("empty");
        let (mut wal, _rx) = Wal::with_options(opts(tmp.path())).unwrap();
        assert!(matches!(
            wal.append(b"", AppendConfig::no_flush()),
            Err(WalError::EmptyPayload)
        ));
    }

    #[test]
    fn truncates_torn_trailing_record() {
        let tmp = TempDir::new("torn");
        let path = tmp.path().join("segment.wal");
        {
            let (mut wal, _rx) = Wal::with_options(opts(tmp.path())).unwrap();
            wal.append(b"ok", AppendConfig::flush_all()).unwrap();
        }
        // Append a torn record: length says 8 bytes, only 3 present.
        {
            use std::fs::OpenOptions;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&8u32.to_le_bytes()).unwrap();
            f.write_all(b"abc").unwrap();
            f.sync_all().unwrap();
        }
        let (wal, _rx) = Wal::with_options(opts(tmp.path())).unwrap();
        assert_eq!(wal.logical_len(), 4 + 2);
    }

    #[test]
    fn preallocate_then_append() {
        let tmp = TempDir::new("prealloc");
        let options = WalOptions::new(tmp.path(), "segment.wal")
            .preallocate_bytes(4096)
            .max_file_bytes(0)
            .notify_capacity(8);
        let (mut wal, _rx) = Wal::with_options(options).unwrap();
        wal.append(b"x", AppendConfig::flush_data()).unwrap();
        assert!(wal.file_len >= 4096);
        assert_eq!(wal.logical_len(), 5);

        let (wal2, _rx) = Wal::with_options(
            WalOptions::new(tmp.path(), "segment.wal")
                .preallocate_bytes(4096)
                .max_file_bytes(0)
                .notify_capacity(8),
        )
        .unwrap();
        assert_eq!(wal2.logical_len(), 5);
    }

    #[test]
    fn segment_full_is_reported() {
        let tmp = TempDir::new("full");
        let options = WalOptions::new(tmp.path(), "segment.wal")
            .without_preallocate()
            .max_file_bytes(10)
            .notify_capacity(4);
        let (mut wal, _rx) = Wal::with_options(options).unwrap();
        // 4 + 3 = 7 fits; next 4+3 = 7 would need 14 total.
        wal.append(b"abc", AppendConfig::no_flush()).unwrap();
        assert!(matches!(
            wal.append(b"def", AppendConfig::no_flush()),
            Err(WalError::SegmentFull { .. })
        ));
    }
}
