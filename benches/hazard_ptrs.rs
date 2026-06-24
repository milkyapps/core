//! Benchmarks for `HazardPointers`' `protect` / `unprotect` / `retire` /
//! `finish` operations.
//!
//! These benchmarks exist primarily as a profiling target for
//! `cargo flamegraph` (see the `flamegraph` justfile recipe). Each bench
//! tight-loops one of the four operations so that the operation dominates the
//! captured samples and shows up clearly in the flamegraph.

use micromeasure::{NoContext, Throughput, benchmark_main, black_box};
use milkyapps_core::hazard_ptrs::HazardPointers;

/// Tight-loop of `protect` followed by `unprotect` on a single pointer.
fn protect_unprotect_cycle(_ctx: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    let hp = HazardPointers::<u64>::with_capacity(8, 8);
    let local = hp.local().unwrap();

    let mut value = Box::new(42_u64);
    let ptr = value.as_mut() as *mut u64;

    for _ in 0..chunk_size {
        let guard = local.protect(ptr).unwrap();
        guard.unprotect();
    }

    local.finish();
    black_box(value);
}

/// Tight-loop of `protect` followed by `retire`, then a single `reclaim` to
/// release the accumulated retire nodes (so the bench does not leak memory
/// across repeated runs).
///
/// `reclaim` itself is exercised here only to free the retire nodes; it is not
/// one of the four profiled methods but is required to keep `retire` honest.
fn retire_cycle(_ctx: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    let hp = HazardPointers::<u64>::with_capacity(8, 8);
    let local = hp.local().unwrap();

    let mut value = Box::new(42_u64);
    let ptr = value.as_mut() as *mut u64;

    for _ in 0..chunk_size {
        let guard = local.protect(ptr).unwrap();
        guard.retire();
    }

    // The retire nodes were all built around the same (now unprotected) `ptr`,
    // so `reclaim` reclaims every node and frees them. We still own `value`,
    // so the returned pointers are ignored (no double-free).
    let mut reclaimed: Vec<*mut u64> = Vec::new();
    hp.reclaim(&mut reclaimed);
    black_box(reclaimed.len());

    local.finish();
    black_box(value);
}

/// Tight-loop of `local` + `protect` + `unprotect` + `finish`, so that
/// `finish` (slot clear + availability CAS) is exercised on every iteration.
fn finish_cycle(_ctx: &mut NoContext, chunk_size: usize, _chunk_num: usize) {
    let hp = HazardPointers::<u64>::with_capacity(8, 8);

    let mut value = Box::new(42_u64);
    let ptr = value.as_mut() as *mut u64;

    for _ in 0..chunk_size {
        let local = hp.local().unwrap();
        let guard = local.protect(ptr).unwrap();
        guard.unprotect();
        local.finish();
    }

    black_box(value);
}

benchmark_main!(|runner| {
    runner.group::<NoContext>("hazard_ptrs", |g| {
        g.throughput(Throughput::per_operation(1, "cycle"))
            .bench("protect_unprotect", protect_unprotect_cycle);
        g.throughput(Throughput::per_operation(1, "cycle"))
            .bench("retire", retire_cycle);
        g.throughput(Throughput::per_operation(1, "cycle"))
            .bench("finish", finish_cycle);
    });
});
