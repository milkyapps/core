ci:
    cargo fmt
    cargo clippy --all-features --all-targets -- -D warnings
    cargo t
    cargo +nightly miri test
    cargo bench
    cargo doc
