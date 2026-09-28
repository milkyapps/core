//! Snapshot-driven integration tests for [`milkyapps_core::collections::arttrie`].
//!
//! Each `tests/arttrie/*.toml` file becomes one libtest-mimic trial. The runner
//! opens a fresh [`ArtTrie`], executes every command in order, and after each
//! step appends a visual dump to a transcript asserted with `cargo-insta`.
//!
//! # TOML format
//!
//! ```toml
//! page_size = 65536               # optional, default 65536
//! dirties_before_flush = 16       # optional, default 16
//! include_pages = true            # optional; false = trie + root only (no page hex)
//!
//! [[cmds]]
//! op = "insert"
//! key = "hello"
//! value = "world"
//!
//! [[cmds]]
//! op = "get"
//! key = "hello"
//!
//! # Bulk insert single-byte keys `from..=to` (useful for node growth).
//! [[cmds]]
//! op = "insert_bytes"
//! from = 0
//! to = 16
//! value = "v"
//!
//! # Bulk insert keys `"{prefix}{i}"` for i in from..=to.
//! [[cmds]]
//! op = "insert_seq"
//! prefix = "k"
//! from = 0
//! to = 4
//! value_prefix = "v"
//! ```

use libtest_mimic::{Arguments, Failed, Trial};
use milkyapps_core::collections::arttrie::ArtTrie;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let args = Arguments::from_args();
    let tests = discover_tests().unwrap_or_else(|e| {
        eprintln!("failed to discover arttrie fixtures: {e}");
        Vec::new()
    });
    libtest_mimic::run(&args, tests).exit();
}

fn discover_tests() -> Result<Vec<Trial>, Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/arttrie");
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
        let test_name = format!("arttrie::{name}");
        trials.push(Trial::test(test_name, move || run_fixture(&path)));
    }
    Ok(trials)
}

fn run_fixture(path: &Path) -> Result<(), Failed> {
    let text = fs::read_to_string(path)
        .map_err(|e| Failed::from(format!("read {}: {e}", path.display())))?;
    let fixture: Fixture =
        toml::from_str(&text).map_err(|e| Failed::from(format!("parse {}: {e}", path.display())))?;

    let dir = tempfile::tempdir().map_err(|e| Failed::from(e.to_string()))?;
    let db_path = dir.path().join("art.db");

    let page_size = fixture.page_size.unwrap_or(65536);
    let dirties = fixture.dirties_before_flush.unwrap_or(16);
    let include_pages = fixture.include_pages.unwrap_or(true);
    let mut trie = ArtTrie::open_with_options(&db_path, page_size, dirties)
        .map_err(|e| Failed::from(format!("open: {e}")))?;

    let mut transcript = String::new();
    transcript.push_str("> Start\n\n");
    transcript.push_str(&trie.debug_dump_with_pages(include_pages));
    transcript.push('\n');

    for (i, cmd) in fixture.cmds.iter().enumerate() {
        let label = cmd.label();
        transcript.push_str(&format!("> {label}\n\n"));

        apply_cmd(&mut trie, cmd, &mut transcript)
            .map_err(|e| Failed::from(format!("cmd {i} ({label}): {e}")))?;

        transcript.push_str(&trie.debug_dump_with_pages(include_pages));
        transcript.push('\n');
    }

    let snap_name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("fixture");
    insta::assert_snapshot!(snap_name, transcript);
    Ok(())
}

fn apply_cmd(trie: &mut ArtTrie, cmd: &Cmd, transcript: &mut String) -> Result<(), String> {
    match cmd {
        Cmd::Insert { key, value } => {
            trie.insert(key.as_bytes(), value.as_bytes());
            Ok(())
        }
        Cmd::Get { key } => {
            let got = trie.get(key.as_bytes());
            let value_line = match got.value {
                None => "result: None\n".to_string(),
                Some(v) => format!("result: Some({:?})\n", String::from_utf8_lossy(v)),
            };
            transcript.push_str(&value_line);
            transcript.push_str(&got.stats.debug_dump());
            transcript.push('\n');
            Ok(())
        }
        Cmd::InsertBytes { from, to, value } => {
            if *to < *from {
                return Err(format!("insert_bytes: to ({to}) < from ({from})"));
            }
            for b in *from..=*to {
                trie.insert(&[b], value.as_bytes());
            }
            Ok(())
        }
        Cmd::InsertSeq {
            prefix,
            from,
            to,
            value_prefix,
        } => {
            if *to < *from {
                return Err(format!("insert_seq: to ({to}) < from ({from})"));
            }
            for i in *from..=*to {
                let key = format!("{prefix}{i}");
                let value = format!("{value_prefix}{i}");
                trie.insert(key.as_bytes(), value.as_bytes());
            }
            Ok(())
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
    include_pages: Option<bool>,
    #[serde(default)]
    cmds: Vec<Cmd>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Cmd {
    Insert { key: String, value: String },
    Get { key: String },
    /// Insert one key per byte in `from..=to` (key is a single byte).
    InsertBytes { from: u8, to: u8, value: String },
    /// Insert keys `"{prefix}{i}"` → `"{value_prefix}{i}"` for i in from..=to.
    InsertSeq {
        prefix: String,
        from: u32,
        to: u32,
        value_prefix: String,
    },
}

impl Cmd {
    fn label(&self) -> String {
        match self {
            Cmd::Insert { key, value } => format!("insert key={key:?} value={value:?}"),
            Cmd::Get { key } => format!("get key={key:?}"),
            Cmd::InsertBytes { from, to, value } => {
                format!("insert_bytes from={from} to={to} value={value:?}")
            }
            Cmd::InsertSeq {
                prefix,
                from,
                to,
                value_prefix,
            } => format!(
                "insert_seq prefix={prefix:?} from={from} to={to} value_prefix={value_prefix:?}"
            ),
        }
    }
}
