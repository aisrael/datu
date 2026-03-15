use std::process::Command;

use criterion::Criterion;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use dirs::home_dir;

/// Expand ~ in the path to the home directory
fn expand_path(path: &str) -> eyre::Result<String> {
    if path.starts_with("~") {
        let home = home_dir().ok_or(eyre::eyre!("Failed to get home directory"))?;
        Ok(home
            .join(path.trim_start_matches("~"))
            .to_string_lossy()
            .to_string())
    } else {
        Ok(path.to_string())
    }
}

fn parquet_to_avro_benchmark(c: &mut Criterion) {
    let datu_path = std::env::var("CARGO_BIN_EXE_datu")
        .expect("CARGO_BIN_EXE_datu must be set when running benchmarks");
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let output_path = temp_dir.path().join("output.avro");
    let output = output_path.to_str().expect("Path to string").to_string();

    let mut id = String::from("parquet_to_avro");
    let input_path = if let Ok(value) = std::env::var("DATU_BENCH_PARQUET_PATH") {
        let path = expand_path(&value).unwrap_or_else(|e| {
            panic!("Failed to expand path: {}", e);
        });
        if !std::path::Path::new(&path).exists() {
            panic!("DATU_BENCH_PARQUET_PATH does not exist: {}", value);
        }
        id = format!("parquet_to_avro ({})", value);
        path
    } else {
        "fixtures/userdata.parquet".to_string()
    };
    c.bench_function(id.as_str(), |b| {
        b.iter(|| {
            let result = Command::new(&datu_path)
                .args(["convert", &input_path, black_box(&output)])
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
