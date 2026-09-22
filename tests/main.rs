//! Snapshot-driven integration tests for [`milkyapps_core::pagemgr`].
//!
//! Each `tests/pagemgr/*.toml` file becomes one libtest-mimic trial. The runner
//! opens a fresh [`PageManager`], executes every command in order, and after
//! each step appends a visual dump to a transcript that is asserted with
//! `cargo-insta`.
//!
//! # TOML format
//!
//! ```toml
//! page_size = 512                 # optional, default 4096
//! dirties_before_flush = 16       # optional, default 16
//!
//! [[cmds]]
//! op = "alloc"
//! name = "a"                      # handle used by later dealloc / flush_page
//! data = "hello"                  # bytes written into the slot (zero-padded)
//!
//! [[cmds]]
//! op = "dealloc"
//! name = "a"
//!
//! [[cmds]]
//! op = "flush"                    # flush_all
//!
//! [[cmds]]
//! op = "flush_dirty"
//!
//! [[cmds]]
//! op = "flush_page"
//! name = "a"                      # flush the page that holds allocation `a`
//! ```
//!
//! Legacy shorthand used by early fixtures is also accepted:
//!
//! ```toml
//! [[cmds.alloc1]]
//! data = "123456789A"
//! ```
//!
//! which is equivalent to `op = "alloc", name = "alloc1", …` (one entry per
//! array element, in file order).

use libtest_mimic::{Arguments, Failed, Trial};
use milkyapps_core::pagemgr::{PageManager, DEFAULT_PAGE_SIZE};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let args = Arguments::from_args();
    let tests = discover_tests().unwrap_or_else(|e| {
        eprintln!("failed to discover pagemgr fixtures: {e}");
        Vec::new()
    });
    libtest_mimic::run(&args, tests).exit();
}

fn discover_tests() -> Result<Vec<Trial>, Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/pagemgr");
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
        let test_name = format!("pagemgr::{name}");
        trials.push(Trial::test(test_name, move || run_fixture(&path)));
    }
    Ok(trials)
}

fn run_fixture(path: &Path) -> Result<(), Failed> {
    let text = fs::read_to_string(path)
        .map_err(|e| Failed::from(format!("read {}: {e}", path.display())))?;
    let fixture: Fixture = parse_fixture(&text)
        .map_err(|e| Failed::from(format!("parse {}: {e}", path.display())))?;

    let dir = tempfile::tempdir().map_err(|e| Failed::from(e.to_string()))?;
    let db_path = dir.path().join("store.db");

    let page_size = fixture.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    let dirties = fixture.dirties_before_flush.unwrap_or(16);
    let mut mgr = PageManager::open_with_options(&db_path, page_size, dirties)
        .map_err(|e| Failed::from(format!("open: {e}")))?;

    let mut transcript = String::new();
    transcript.push_str("> Start\n\n");
    transcript.push_str(&mgr.debug_dump());
    transcript.push('\n');

    let mut handles: HashMap<String, milkyapps_core::pagemgr::AllocId> = HashMap::new();

    for (i, cmd) in fixture.cmds.iter().enumerate() {
        let label = cmd.label();
        transcript.push_str(&format!("> {label}\n\n"));

        apply_cmd(&mut mgr, &mut handles, cmd)
            .map_err(|e| Failed::from(format!("cmd {i} ({label}): {e}")))?;

        transcript.push_str(&mgr.debug_dump());
        transcript.push('\n');
    }

    let snap_name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("fixture");
    // Bind the snapshot to the fixture stem so each toml file gets its own file.
    insta::assert_snapshot!(snap_name, transcript);
    Ok(())
}

fn apply_cmd(
    mgr: &mut PageManager,
    handles: &mut HashMap<String, milkyapps_core::pagemgr::AllocId>,
    cmd: &Cmd,
) -> Result<(), String> {
    match cmd {
        Cmd::Alloc { name, data } => {
            let bytes = data.as_bytes();
            let need = bytes.len().max(1).next_power_of_two();
            let aid = mgr
                .alloc_slot(need)
                .ok_or_else(|| format!("alloc failed for {need} bytes (name={name})"))?;
            let slot = mgr.slot_bytes_mut(aid);
            slot.fill(0);
            slot[..bytes.len()].copy_from_slice(bytes);
            handles.insert(name.clone(), aid);
            Ok(())
        }
        Cmd::Dealloc { name } => {
            let aid = handles
                .remove(name)
                .ok_or_else(|| format!("unknown alloc handle '{name}'"))?;
            mgr.dealloc(aid);
            Ok(())
        }
        Cmd::Flush => mgr.flush_all().map_err(|e| e.to_string()),
        Cmd::FlushDirty => mgr.flush_dirty().map_err(|e| e.to_string()),
        Cmd::FlushPage { name } => {
            let aid = handles
                .get(name)
                .ok_or_else(|| format!("unknown alloc handle '{name}'"))?;
            mgr.flush_page(aid.page_id()).map_err(|e| e.to_string())
        }
    }
}

#[derive(Debug, Deserialize)]
struct Fixture {
    #[serde(default)]
    page_size: Option<usize>,
    #[serde(default)]
    dirties_before_flush: Option<usize>,
    #[serde(default)]
    cmds: Vec<Cmd>,
}

#[derive(Debug, Clone)]
enum Cmd {
    Alloc { name: String, data: String },
    Dealloc { name: String },
    Flush,
    FlushDirty,
    FlushPage { name: String },
}

impl Cmd {
    fn label(&self) -> String {
        match self {
            Cmd::Alloc { name, data } => format!("alloc name={name} data={data:?}"),
            Cmd::Dealloc { name } => format!("dealloc name={name}"),
            Cmd::Flush => "flush".to_string(),
            Cmd::FlushDirty => "flush_dirty".to_string(),
            Cmd::FlushPage { name } => format!("flush_page name={name}"),
        }
    }
}

impl<'de> Deserialize<'de> for Cmd {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            op: String,
            #[serde(default)]
            name: Option<String>,
            #[serde(default)]
            data: Option<String>,
        }

        let raw = Raw::deserialize(deserializer)?;
        match raw.op.as_str() {
            "alloc" => Ok(Cmd::Alloc {
                name: raw
                    .name
                    .ok_or_else(|| serde::de::Error::missing_field("name"))?,
                data: raw.data.unwrap_or_default(),
            }),
            "dealloc" => Ok(Cmd::Dealloc {
                name: raw
                    .name
                    .ok_or_else(|| serde::de::Error::missing_field("name"))?,
            }),
            "flush" | "flush_all" => Ok(Cmd::Flush),
            "flush_dirty" => Ok(Cmd::FlushDirty),
            "flush_page" => Ok(Cmd::FlushPage {
                name: raw
                    .name
                    .ok_or_else(|| serde::de::Error::missing_field("name"))?,
            }),
            other => Err(serde::de::Error::custom(format!("unknown op '{other}'"))),
        }
    }
}

/// Parse a fixture, accepting either the typed `[[cmds]]` form or the legacy
/// `[[cmds.<name>]]` alloc shorthand.
fn parse_fixture(text: &str) -> Result<Fixture, String> {
    let value: toml::Value = toml::from_str(text).map_err(|e| e.to_string())?;

    let page_size = value
        .get("page_size")
        .and_then(toml::Value::as_integer)
        .map(|n| n as usize);
    let dirties_before_flush = value
        .get("dirties_before_flush")
        .and_then(toml::Value::as_integer)
        .map(|n| n as usize);

    let Some(cmds_val) = value.get("cmds") else {
        return Ok(Fixture {
            page_size,
            dirties_before_flush,
            cmds: Vec::new(),
        });
    };

    // Preferred: `cmds` is an array of tables with an `op` field.
    if let Some(arr) = cmds_val.as_array() {
        if arr
            .first()
            .and_then(|v| v.get("op"))
            .and_then(toml::Value::as_str)
            .is_some()
        {
            let cmds: Vec<Cmd> = arr
                .iter()
                .map(|v| {
                    v.clone()
                        .try_into()
                        .map_err(|e: toml::de::Error| e.to_string())
                })
                .collect::<Result<_, _>>()?;
            return Ok(Fixture {
                page_size,
                dirties_before_flush,
                cmds,
            });
        }
    }

    // Legacy: `cmds` is a table of arrays, e.g. `[[cmds.alloc1]]`.
    if let Some(table) = cmds_val.as_table() {
        let mut cmds = Vec::new();
        for (key, entries) in table {
            let arr = entries
                .as_array()
                .ok_or_else(|| format!("cmds.{key} must be an array of tables"))?;
            match key.as_str() {
                "flush" | "flush_all" => {
                    for _ in arr {
                        cmds.push(Cmd::Flush);
                    }
                }
                "flush_dirty" => {
                    for _ in arr {
                        cmds.push(Cmd::FlushDirty);
                    }
                }
                "dealloc" => {
                    for entry in arr {
                        let name = entry
                            .get("name")
                            .and_then(toml::Value::as_str)
                            .ok_or_else(|| "dealloc requires name".to_string())?
                            .to_string();
                        cmds.push(Cmd::Dealloc { name });
                    }
                }
                "flush_page" => {
                    for entry in arr {
                        let name = entry
                            .get("name")
                            .and_then(toml::Value::as_str)
                            .ok_or_else(|| "flush_page requires name".to_string())?
                            .to_string();
                        cmds.push(Cmd::FlushPage { name });
                    }
                }
                name => {
                    // Treat any other key as an alloc handle name.
                    for entry in arr {
                        let data = entry
                            .get("data")
                            .and_then(toml::Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        cmds.push(Cmd::Alloc {
                            name: name.to_string(),
                            data,
                        });
                    }
                }
            }
        }
        return Ok(Fixture {
            page_size,
            dirties_before_flush,
            cmds,
        });
    }

    Err("cmds must be an array of command tables or a legacy table of arrays".into())
}
