use std::process::Command;

use criterion::Criterion;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;

fn parquet_to_avro_benchmark(c: &mut Criterion) {
    let datu_path = std::env::var("CARGO_BIN_EXE_datu")
        .expect("CARGO_BIN_EXE_datu must be set when running benchmarks");
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let output_path = temp_dir.path().join("output.avro");
    let output = output_path.to_str().expect("Path to string").to_string();

    c.bench_function("parquet_to_avro_userdata", |b| {
        b.iter(|| {
            let result = Command::new(&datu_path)
                .args(["convert", "fixtures/userdata.parquet", black_box(&output)])
                .output()
                .expect("Failed to execute datu");
            assert!(
                result.status.success(),
                "datu convert failed: stdout={} stderr={}",
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            );
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(500);
    targets = parquet_to_avro_benchmark
}
criterion_main!(benches);
