//! Deterministic simulation testing (DST) world.
//!
//! Provides an in-memory [`FsApi`] with explicit durability (pending vs
//! synced bytes), a seeded RNG, an op log, and helpers to open storage
//! components against the simulated filesystem.
//!
//! # Crash model
//!
//! - Writes update **volatile** file bytes immediately.
//! - [`FsApi::sync_data`] / [`FsApi::sync_all`] copy volatile → durable.
//! - [`FsApi::sync_dir`] makes directory entries durable.
//! - [`Sim::crash`] discards volatile state: files and names revert to the
//!   last durable snapshot (Pillai-style “keep only what was synced”).
//!
//! Page files used by [`crate::storage::PageManager`] memory backends are
//! tracked separately via [`Sim::install_pages`] / [`Sim::pages_durable`].

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, IoSlice, SeekFrom};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::storage::wal::FsApi;

/// Seeded xorshift64* RNG for schedule / fault choices.
#[derive(Debug, Clone)]
pub struct SimRng {
    state: u64,
}

impl SimRng {
    /// Create an RNG from `seed` (`0` is mapped to a non-zero constant).
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed },
        }
    }

    /// Next `u64` in the stream.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform `usize` in `0..max` (`max == 0` → `0`).
    pub fn gen_index(&mut self, max: usize) -> usize {
        if max == 0 {
            return 0;
        }
        let max_u = u64::try_from(max).unwrap_or(u64::MAX);
        usize::try_from(self.next_u64() % max_u).unwrap_or(0)
    }

    /// Current seed state (for logging / reproduction).
    #[must_use]
    pub const fn state(&self) -> u64 {
        self.state
    }
}

/// One recorded simulation step (printed on failure).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimOp {
    /// Monotonic step index.
    pub step: u64,
    /// Human-readable description.
    pub detail: String,
}

/// Shared simulated filesystem + page-file durable images.
#[derive(Debug, Clone)]
pub struct Sim {
    inner: Rc<RefCell<SimInner>>,
}

#[derive(Debug)]
struct SimInner {
    seed: u64,
    rng: SimRng,
    log: Vec<SimOp>,
    step: u64,
    /// Durable directory names that survive crash.
    durable_dirs: BTreeSet<PathBuf>,
    /// Volatile directory names (created but maybe not `sync_dir`'d).
    volatile_dirs: BTreeSet<PathBuf>,
    files: BTreeMap<PathBuf, SimFileState>,
    /// Durable page-manager images keyed by path (memory backend).
    pages: BTreeMap<PathBuf, Vec<u8>>,
    /// When true, the next `sync_*` returns an error.
    fail_next_sync: Option<io::ErrorKind>,
}

#[derive(Debug, Clone)]
struct SimFileState {
    volatile: Vec<u8>,
    durable: Vec<u8>,
    /// Directory entry is durable (survives crash).
    name_durable: bool,
}

impl Sim {
    /// Create a simulation world with the given seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            inner: Rc::new(RefCell::new(SimInner {
                seed,
                rng: SimRng::new(seed),
                log: Vec::new(),
                step: 0,
                durable_dirs: BTreeSet::new(),
                volatile_dirs: BTreeSet::new(),
                files: BTreeMap::new(),
                pages: BTreeMap::new(),
                fail_next_sync: None,
            })),
        }
    }

    /// Original seed passed to [`Self::new`].
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.inner.borrow().seed
    }

    /// Cloneable [`FsApi`] handle sharing this world's file state.
    #[must_use]
    pub fn fs(&self) -> SimFs {
        SimFs {
            inner: Rc::clone(&self.inner),
        }
    }

    /// Mutable access to the seeded RNG.
    #[must_use]
    pub fn rng(&self) -> std::cell::RefMut<'_, SimRng> {
        std::cell::RefMut::map(self.inner.borrow_mut(), |i| &mut i.rng)
    }

    /// Append a step to the op log.
    pub fn log(&self, detail: impl Into<String>) {
        let mut inner = self.inner.borrow_mut();
        let step = inner.step;
        inner.step += 1;
        inner.log.push(SimOp {
            step,
            detail: detail.into(),
        });
    }

    /// Borrow the op log.
    #[must_use]
    pub fn ops(&self) -> Vec<SimOp> {
        self.inner.borrow().log.clone()
    }

    /// Format seed + op log for failure messages.
    #[must_use]
    pub fn failure_report(&self) -> String {
        let inner = self.inner.borrow();
        let mut out = format!("DST failure seed={}\nops:\n", inner.seed);
        for op in &inner.log {
            out.push_str(&format!("  [{:>4}] {}\n", op.step, op.detail));
        }
        out
    }

    /// Next sync call fails with `kind` (cleared after one use).
    pub fn fail_next_sync(&self, kind: io::ErrorKind) {
        self.inner.borrow_mut().fail_next_sync = Some(kind);
        self.log(format!("fail_next_sync({kind:?})"));
    }

    /// Install / replace the durable page-file image at `path`.
    pub fn install_pages(&self, path: impl AsRef<Path>, durable: Vec<u8>) {
        let path = normalize(path.as_ref());
        self.log(format!(
            "install_pages({}, {} bytes)",
            path.display(),
            durable.len()
        ));
        self.inner.borrow_mut().pages.insert(path, durable);
    }

    /// Durable page-file bytes at `path`, if any.
    #[must_use]
    pub fn pages_durable(&self, path: impl AsRef<Path>) -> Option<Vec<u8>> {
        let path = normalize(path.as_ref());
        self.inner.borrow().pages.get(&path).cloned()
    }

    /// Crash: revert all files and dirs to their last durable snapshot.
    ///
    /// Callers must drop live [`crate::storage::Wal`] / [`crate::storage::Storage`]
    /// handles first so reopen observes the post-crash state.
    pub fn crash(&self) {
        self.log("crash");
        let mut inner = self.inner.borrow_mut();
        inner.volatile_dirs = inner.durable_dirs.clone();
        let paths: Vec<PathBuf> = inner.files.keys().cloned().collect();
        for path in paths {
            let Some(file) = inner.files.get_mut(&path) else {
                continue;
            };
            if file.name_durable {
                file.volatile = file.durable.clone();
            } else {
                inner.files.remove(&path);
            }
        }
        // Page images in `pages` are already durable-only snapshots.
    }

    /// Run `f` and, on panic or `Err`, attach seed + op log to the message.
    ///
    /// # Errors
    ///
    /// Propagates `f`'s error after annotating it with [`Self::failure_report`].
    pub fn run<E, F>(&self, f: F) -> Result<(), String>
    where
        E: std::fmt::Display,
        F: FnOnce(&Sim) -> Result<(), E>,
    {
        match f(self) {
            Ok(()) => Ok(()),
            Err(e) => Err(format!("{e}\n{}", self.failure_report())),
        }
    }
}

/// [`FsApi`] backed by a [`Sim`] world.
#[derive(Debug, Clone)]
pub struct SimFs {
    inner: Rc<RefCell<SimInner>>,
}

/// File handle for [`SimFs`].
#[derive(Debug)]
pub struct SimFile {
    path: PathBuf,
    cursor: u64,
}

fn normalize(path: &Path) -> PathBuf {
    // Keep paths comparable across joins; do not require canonicalization.
    path.to_path_buf()
}

fn take_sync_fault(inner: &mut SimInner) -> io::Result<()> {
    if let Some(kind) = inner.fail_next_sync.take() {
        return Err(io::Error::new(kind, "simulated sync failure"));
    }
    Ok(())
}

impl FsApi for SimFs {
    type File = SimFile;

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        let dir = normalize(dir);
        let mut inner = self.inner.borrow_mut();
        let step = inner.step;
        inner.step += 1;
        inner.log.push(SimOp {
            step,
            detail: format!("create_dir_all({})", dir.display()),
        });
        // Mark dir and parents as volatile.
        let mut cur = PathBuf::new();
        for comp in dir.components() {
            cur.push(comp);
            inner.volatile_dirs.insert(cur.clone());
        }
        Ok(())
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        let path = normalize(path);
        let inner = self.inner.borrow();
        Ok(inner.files.contains_key(&path) || inner.volatile_dirs.contains(&path))
    }

    fn open_segment(&self, path: &Path) -> io::Result<Self::File> {
        let path = normalize(path);
        let mut inner = self.inner.borrow_mut();
        let step = inner.step;
        inner.step += 1;
        inner.log.push(SimOp {
            step,
            detail: format!("open_segment({})", path.display()),
        });
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !inner.volatile_dirs.contains(parent) {
                // Auto-create parent like typical create/open flows after create_dir_all.
                let mut cur = PathBuf::new();
                for comp in parent.components() {
                    cur.push(comp);
                    inner.volatile_dirs.insert(cur.clone());
                }
            }
        }
        inner.files.entry(path.clone()).or_insert_with(|| SimFileState {
            volatile: Vec::new(),
            durable: Vec::new(),
            name_durable: false,
        });
        Ok(SimFile {
            path,
            cursor: 0,
        })
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let dir = normalize(dir);
        let mut inner = self.inner.borrow_mut();
        take_sync_fault(&mut inner)?;
        let step = inner.step;
        inner.step += 1;
        inner.log.push(SimOp {
            step,
            detail: format!("sync_dir({})", dir.display()),
        });
        // Persist the directory and any file names directly under it.
        if inner.volatile_dirs.contains(&dir) {
            inner.durable_dirs.insert(dir.clone());
        }
        let children: Vec<PathBuf> = inner
            .files
            .keys()
            .filter(|p| p.parent() == Some(dir.as_path()))
            .cloned()
            .collect();
        for child in children {
            if let Some(f) = inner.files.get_mut(&child) {
                f.name_durable = true;
            }
        }
        Ok(())
    }

    fn len(&self, file: &Self::File) -> io::Result<u64> {
        let inner = self.inner.borrow();
        let state = inner
            .files
            .get(&file.path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sim file missing"))?;
        Ok(state.volatile.len() as u64)
    }

    fn set_len(&self, file: &Self::File, len: u64) -> io::Result<()> {
        let mut inner = self.inner.borrow_mut();
        let step = inner.step;
        inner.step += 1;
        inner.log.push(SimOp {
            step,
            detail: format!("set_len({}, {len})", file.path.display()),
        });
        let state = inner
            .files
            .get_mut(&file.path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sim file missing"))?;
        let len = usize::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "len too large"))?;
        if state.volatile.len() < len {
            state.volatile.resize(len, 0);
        } else {
            state.volatile.truncate(len);
        }
        Ok(())
    }

    fn seek(&self, file: &mut Self::File, pos: SeekFrom) -> io::Result<u64> {
        let len = {
            let inner = self.inner.borrow();
            let state = inner
                .files
                .get(&file.path)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sim file missing"))?;
            state.volatile.len() as u64
        };
        let next = match pos {
            SeekFrom::Start(o) => o,
            SeekFrom::End(o) => {
                if o >= 0 {
                    len.checked_add(o as u64).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "seek overflow")
                    })?
                } else {
                    let sub = (-o) as u64;
                    len.checked_sub(sub).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "seek before start")
                    })?
                }
            }
            SeekFrom::Current(o) => {
                if o >= 0 {
                    file.cursor.checked_add(o as u64).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "seek overflow")
                    })?
                } else {
                    let sub = (-o) as u64;
                    file.cursor.checked_sub(sub).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "seek before start")
                    })?
                }
            }
        };
        file.cursor = next;
        Ok(next)
    }

    fn read(&self, file: &mut Self::File, buf: &mut [u8]) -> io::Result<usize> {
        let inner = self.inner.borrow();
        let state = inner
            .files
            .get(&file.path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sim file missing"))?;
        let start = usize::try_from(file.cursor)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cursor too large"))?;
        if start >= state.volatile.len() {
            return Ok(0);
        }
        let n = buf.len().min(state.volatile.len() - start);
        buf[..n].copy_from_slice(&state.volatile[start..start + n]);
        drop(inner);
        file.cursor += n as u64;
        Ok(n)
    }

    fn write_all_vectored(
        &self,
        file: &mut Self::File,
        bufs: &mut [IoSlice<'_>],
    ) -> io::Result<()> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        let mut inner = self.inner.borrow_mut();
        let step = inner.step;
        inner.step += 1;
        inner.log.push(SimOp {
            step,
            detail: format!("write({}, {total} bytes)", file.path.display()),
        });
        let state = inner
            .files
            .get_mut(&file.path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sim file missing"))?;
        let start = usize::try_from(file.cursor)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cursor too large"))?;
        let end = start
            .checked_add(total)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write overflow"))?;
        if state.volatile.len() < end {
            state.volatile.resize(end, 0);
        }
        let mut off = start;
        for buf in bufs.iter() {
            let n = buf.len();
            state.volatile[off..off + n].copy_from_slice(buf);
            off += n;
        }
        drop(inner);
        file.cursor += total as u64;
        Ok(())
    }

    fn sync_data(&self, file: &Self::File) -> io::Result<()> {
        let mut inner = self.inner.borrow_mut();
        take_sync_fault(&mut inner)?;
        let step = inner.step;
        inner.step += 1;
        inner.log.push(SimOp {
            step,
            detail: format!("sync_data({})", file.path.display()),
        });
        let state = inner
            .files
            .get_mut(&file.path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sim file missing"))?;
        state.durable = state.volatile.clone();
        Ok(())
    }

    fn sync_all(&self, file: &Self::File) -> io::Result<()> {
        let mut inner = self.inner.borrow_mut();
        take_sync_fault(&mut inner)?;
        let step = inner.step;
        inner.step += 1;
        inner.log.push(SimOp {
            step,
            detail: format!("sync_all({})", file.path.display()),
        });
        let state = inner
            .files
            .get_mut(&file.path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sim file missing"))?;
        state.durable = state.volatile.clone();
        state.name_durable = true;
        if let Some(parent) = file.path.parent() {
            if !parent.as_os_str().is_empty() {
                inner.durable_dirs.insert(parent.to_path_buf());
                inner.volatile_dirs.insert(parent.to_path_buf());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::{AppendConfig, FlushStrategy, Wal, WalOptions};

    #[test]
    fn wal_crash_loses_unflushed_append() {
        let sim = Sim::new(1);
        let dir = PathBuf::from("/sim/wal_crash");
        let opts = WalOptions::new(&dir, "segment.wal")
            .without_preallocate()
            .max_file_bytes(0)
            .notify_capacity(8);

        sim.run(|sim| -> Result<(), String> {
            {
                let (mut wal, _rx) =
                    Wal::with_options_and_fs(opts.clone(), sim.fs()).map_err(|e| e.to_string())?;
                wal.append(b"durable", AppendConfig::flush_all())
                    .map_err(|e| e.to_string())?;
                wal.append(b"volatile", AppendConfig::no_flush())
                    .map_err(|e| e.to_string())?;
                sim.log("appended durable+volatile");
            }
            sim.crash();
            let (mut wal, _rx) =
                Wal::with_options_and_fs(opts.clone(), sim.fs()).map_err(|e| e.to_string())?;
            let records = wal.records().map_err(|e| e.to_string())?;
            if records.len() != 1 || records[0].1 != b"durable" {
                return Err(format!("expected only durable record, got {records:?}"));
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn wal_flush_then_crash_keeps_record() {
        let sim = Sim::new(2);
        let dir = PathBuf::from("/sim/wal_keep");
        let opts = WalOptions::new(&dir, "segment.wal")
            .without_preallocate()
            .max_file_bytes(0)
            .notify_capacity(4);

        {
            let (mut wal, _rx) = Wal::with_options_and_fs(opts.clone(), sim.fs()).unwrap();
            wal.append(b"x", AppendConfig::no_flush()).unwrap();
            wal.flush(FlushStrategy::All).unwrap();
        }
        sim.crash();
        let (mut wal, _rx) = Wal::with_options_and_fs(opts, sim.fs()).unwrap();
        let records = wal.records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, b"x");
    }
}
