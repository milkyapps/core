use micromeasure::{NoContext, Throughput, benchmark_main, black_box};

fn position_of_any_bool_simd(_ctx: &mut NoContext, _chunk_size: usize, _chunk_num: usize) {
    // Find first
    let buffer = vec![false; _chunk_size];
    let pos = milkyapps_core::simd::position_of_any_bool(buffer.as_ref(), false);
    black_box(pos);

    // Find last
    let mut buffer = vec![false; _chunk_size];
    buffer[_chunk_size - 1] = true;
    let pos = milkyapps_core::simd::position_of_any_bool(buffer.as_ref(), true);
    black_box(pos);
}

fn position_of_any_bool_iter(_ctx: &mut NoContext, _chunk_size: usize, _chunk_num: usize) {
    // Find first
    let buffer = vec![false; _chunk_size];
    let pos = buffer.iter().position(|x| !*x);
    black_box(pos);

    // Find last
    let mut buffer = vec![false; _chunk_size];
    buffer[_chunk_size - 1] = true;
    let pos = buffer.iter().position(|x| *x);
    black_box(pos);
}

benchmark_main!(|runner| {
    runner.group::<NoContext>("position_of", |g| {
        g.throughput(Throughput::per_operation(1, "bool"))
            .bench("position_of_any_bool_simd", position_of_any_bool_simd);
        g.throughput(Throughput::per_operation(1, "bool"))
            .bench("position_of_any_bool_iter", position_of_any_bool_iter);
    });
});
