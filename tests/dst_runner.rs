//! Deterministic simulation tests for WAL + Storage.
//!
//! Each `tests/dst/*.toml` file becomes one libtest-mimic trial. Fixtures drive
//! a [`milkyapps_core::sim::Sim`] world with crash / reopen / expect commands.
//!
//! # TOML format
//!
//! ```toml
//! seed = 1
//!
//! [[cmds]]
//! op = "wal_append"
//! payload = "hello"
//! flush = "all"          # none | data | all
//!
//! [[cmds]]
//! op = "crash"
//!
//! [[cmds]]
//! op = "wal_reopen"
//!
//! [[cmds]]
//! op = "wal_expect"
//! records = ["hello"]
//!
//! [[cmds]]
//! op = "storage_put"
//! key = "k"
//! value = "v"
//! flush = "none"
//!
//! [[cmds]]
//! op = "storage_sync_pages"   # publish durable page image into the sim
//!
//! [[cmds]]
//! op = "storage_reopen"
//!
//! [[cmds]]
//! op = "storage_expect_get"
//! key = "k"
//! value = "v"                 # omit or use missing = true for absence
//! ```

use libtest_mimic::{Arguments, Failed, Trial};
use milkyapps_core::sim::Sim;
use milkyapps_core::storage::wal::{
    AppendConfig, FlushStrategy, Wal, WalEvent, WalOptions,
};
use milkyapps_core::storage::{Storage, StorageOptions};
use milkyapps_core::sync::bounded::Receiver;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let args = Arguments::from_args();
    let tests = discover_tests().unwrap_or_else(|e| {
        eprintln!("failed to discover dst fixtures: {e}");
        Vec::new()
    });
    libtest_mimic::run(&args, tests).exit();
}

fn discover_tests() -> Result<Vec<Trial>, Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/dst");
    if !root.is_dir() {
        return Err(format!("missing fixtures directory: {}", root.display()).into());
    }

    let mut paths: Vec<PathBuf> = fs::read_dir(&root)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("toml"))
        .collect();
    paths.sort();

    let mut trials = Vec::with_capacity(paths.len());
    for path in paths {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unnamed")
            .to_string();
        let test_name = format!("dst::{name}");
        trials.push(Trial::test(test_name, move || run_fixture(&path)));
    }
    Ok(trials)
}

fn run_fixture(path: &Path) -> Result<(), Failed> {
    let text = fs::read_to_string(path)
        .map_err(|e| Failed::from(format!("read {}: {e}", path.display())))?;
    let fixture: Fixture =
        toml::from_str(&text).map_err(|e| Failed::from(format!("parse {}: {e}", path.display())))?;

    let sim = Sim::new(fixture.seed.unwrap_or(1));
    match run_cmds(&sim, &fixture) {
        Ok(()) => Ok(()),
        Err(e) => Err(Failed::from(format!("{e}\n{}", sim.failure_report()))),
    }
}

fn run_cmds(sim: &Sim, fixture: &Fixture) -> Result<(), String> {
    let dir = PathBuf::from("/sim/dst");
    let wal_opts = WalOptions::new(&dir, "segment.wal")
        .without_preallocate()
        .max_file_bytes(0)
        .notify_capacity(fixture.notify_capacity.unwrap_or(16));

    let mut storage_opts = StorageOptions::new(&dir);
    storage_opts.page_size = fixture.page_size.unwrap_or(4096);
    storage_opts.dirties_before_flush = fixture.dirties_before_flush.unwrap_or(64);
    storage_opts.wal = wal_opts.clone();

    let mut wal: Option<Wal<_>> = None;
    // Keep the notify receiver alive — dropping it closes the channel and
    // makes subsequent `Wal::append` fail with `NotifyClosed`.
    let mut _wal_notify: Option<Receiver<WalEvent>> = None;
    let mut storage: Option<Storage<_>> = None;

    for cmd in &fixture.cmds {
        match cmd.op.as_str() {
            "wal_append" => {
                let payload = cmd.payload.as_deref().ok_or("wal_append needs payload")?;
                let flush = parse_flush(cmd.flush.as_deref().unwrap_or("none"))?;
                if wal.is_none() {
                    let (w, rx) = Wal::with_options_and_fs(wal_opts.clone(), sim.fs())
                        .map_err(|e| e.to_string())?;
                    wal = Some(w);
                    _wal_notify = Some(rx);
                }
                let w = wal.as_mut().unwrap();
                sim.log(format!("wal_append({payload:?}, {flush:?})"));
                w.append(payload.as_bytes(), AppendConfig { flush })
                    .map_err(|e| e.to_string())?;
            }
            "wal_flush" => {
                let flush = parse_flush(cmd.flush.as_deref().unwrap_or("all"))?;
                let w = wal.as_mut().ok_or("wal_flush without open wal")?;
                sim.log(format!("wal_flush({flush:?})"));
                w.flush(flush).map_err(|e| e.to_string())?;
            }
            "wal_reopen" => {
                drop(wal.take());
                drop(_wal_notify.take());
                sim.log("wal_reopen");
                let (w, rx) = Wal::with_options_and_fs(wal_opts.clone(), sim.fs())
                    .map_err(|e| e.to_string())?;
                wal = Some(w);
                _wal_notify = Some(rx);
            }
            "wal_expect" => {
                let expected = cmd.records.clone().unwrap_or_default();
                let w = wal.as_mut().ok_or("wal_expect without open wal")?;
                let records = w.records().map_err(|e| e.to_string())?;
                let got: Vec<String> = records
                    .iter()
                    .map(|(_, b)| String::from_utf8_lossy(b).into_owned())
                    .collect();
                sim.log(format!("wal_expect({expected:?}) got={got:?}"));
                if got != expected {
                    return Err(format!("wal_expect: expected {expected:?}, got {got:?}"));
                }
            }
            "crash" => {
                // Drop live handles so reopen observes post-crash durable state.
                if let Some(mut s) = storage.take() {
                    s.sync_sim_pages(sim).map_err(|e| e.to_string())?;
                }
                drop(wal.take());
                drop(_wal_notify.take());
                sim.crash();
            }
            "storage_put" => {
                let key = cmd.key.as_deref().ok_or("storage_put needs key")?;
                let value = cmd.value.as_deref().ok_or("storage_put needs value")?;
                let flush = parse_flush(cmd.flush.as_deref().unwrap_or("none"))?;
                if storage.is_none() {
                    storage = Some(
                        Storage::open_sim(sim, storage_opts.clone()).map_err(|e| e.to_string())?,
                    );
                }
                let s = storage.as_mut().unwrap();
                sim.log(format!("storage_put({key:?},{value:?},{flush:?})"));
                s.put(key.as_bytes(), value.as_bytes(), AppendConfig { flush })
                    .map_err(|e| e.to_string())?;
            }
            "storage_flush_pages" => {
                let s = storage.as_mut().ok_or("storage_flush_pages without storage")?;
                sim.log("storage_flush_pages");
                s.flush_pages(true).map_err(|e| e.to_string())?;
                s.sync_sim_pages(sim).map_err(|e| e.to_string())?;
            }
            "storage_sync_pages" => {
                let s = storage.as_mut().ok_or("storage_sync_pages without storage")?;
                sim.log("storage_sync_pages");
                s.sync_sim_pages(sim).map_err(|e| e.to_string())?;
            }
            "storage_reopen" => {
                if let Some(mut s) = storage.take() {
                    s.sync_sim_pages(sim).map_err(|e| e.to_string())?;
                }
                sim.log("storage_reopen");
                storage = Some(
                    Storage::open_sim(sim, storage_opts.clone()).map_err(|e| e.to_string())?,
                );
            }
            "storage_expect_get" => {
                let key = cmd.key.as_deref().ok_or("storage_expect_get needs key")?;
                let missing = cmd.missing.unwrap_or(false);
                let s = storage.as_mut().ok_or("storage_expect_get without storage")?;
                let got = s.get(key.as_bytes());
                sim.log(format!(
                    "storage_expect_get({key:?}, missing={missing}, value={:?})",
                    cmd.value
                ));
                if missing {
                    if got.value.is_some() {
                        return Err(format!(
                            "storage_expect_get: expected missing key {key:?}, found {:?}",
                            got.value.map(|v| String::from_utf8_lossy(v).into_owned())
                        ));
                    }
                } else {
                    let want = cmd
                        .value
                        .as_deref()
                        .ok_or("storage_expect_get needs value or missing=true")?;
                    match got.value {
                        Some(v) if v == want.as_bytes() => {}
                        Some(v) => {
                            return Err(format!(
                                "storage_expect_get: key {key:?} expected {want:?}, got {:?}",
                                String::from_utf8_lossy(v)
                            ));
                        }
                        None => {
                            return Err(format!(
                                "storage_expect_get: key {key:?} missing, expected {want:?}"
                            ));
                        }
                    }
                }
            }
            other => return Err(format!("unknown op {other:?}")),
        }
    }
    Ok(())
}

fn parse_flush(s: &str) -> Result<FlushStrategy, String> {
    match s {
        "none" => Ok(FlushStrategy::None),
        "data" => Ok(FlushStrategy::Data),
        "all" => Ok(FlushStrategy::All),
        other => Err(format!("unknown flush {other:?}")),
    }
}

#[derive(Debug, Deserialize)]
struct Fixture {
    seed: Option<u64>,
    page_size: Option<usize>,
    dirties_before_flush: Option<usize>,
    notify_capacity: Option<usize>,
    cmds: Vec<Cmd>,
}

#[derive(Debug, Deserialize)]
struct Cmd {
    op: String,
    payload: Option<String>,
    flush: Option<String>,
    records: Option<Vec<String>>,
    key: Option<String>,
    value: Option<String>,
    missing: Option<bool>,
}
