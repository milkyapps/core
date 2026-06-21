ci:
    cargo fmt
    cargo clippy --all-features --all-targets -- -D warnings
    cargo t
    cargo bench
    cargo doc
