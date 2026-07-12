ci RUN_LOOM="false":
    cargo fmt
    cargo clippy --all-features --all-targets -- -D warnings
    cargo t
    if {{RUN_LOOM}} == "true"; then just test-loom; fi
    cargo +nightly miri test
    cargo bench
    cargo doc

test-channel:
    cargo t -- channel
    cargo +nightly miri test -- channel
    just test-loom channel

test-loom filter="" $LOOM_LOG="info" :
    RUSTFLAGS="--cfg loom" \
    LOOM_MAX_PREEMPTIONS=2 \
    LOOM_MAX_BRANCHES=100000 \
    LOOM_MAX_PERMUTATIONS=2000 \
    LOOM_MAX_DURATION=30 \
    LOOM_CHECKPOINT_INTERVAL=1000 \
    cargo test --tests -- {{filter}}

flamegraph *ARGS:
    sudo cargo flamegraph --bench hazard_ptrs {{ARGS}}
