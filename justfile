ci RUN_LOOM="false":
    cargo fmt
    cargo clippy --all-features --all-targets -- -D warnings
    cargo t
    if {{RUN_LOOM}} == "true"; then just test-loom; fi
    cargo +nightly miri test
    cargo bench
    cargo doc

test-loom $LOOM_LOG="trace":
    RUSTFLAGS="--cfg loom" \
    LOOM_MAX_PREEMPTIONS=2 \
    LOOM_MAX_BRANCHES=100000 \
    LOOM_MAX_PERMUTATIONS=20000 \
    LOOM_MAX_DURATION=30 \
    LOOM_CHECKPOINT_INTERVAL=1000 \
    LOOM_LOG=trace \
    cargo test --tests
