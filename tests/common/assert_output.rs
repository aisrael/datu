use tempfile::TempDir;

pub const TEMPDIR_PLACEHOLDER: &str = "$TEMPDIR";

pub fn replace_tempdir(s: &str, temp_path: &str) -> String {
    s.replace(TEMPDIR_PLACEHOLDER, temp_path)
}

pub fn assert_output_contains(
    output: &std::process::Output,
    expected: &str,
    temp_dir: Option<&TempDir>,
) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    let expected_resolved = if let Some(temp_dir) = temp_dir {
        let temp_path = temp_dir
            .path()
            .to_str()
            .expect("Temp path is not valid UTF-8");
        replace_tempdir(expected, temp_path)
    } else {
        expected.to_string()
    };

    assert!(
        combined.contains(&expected_resolved),
        "Expected output to contain '{}', but got:\nstdout: {}\nstderr: {}",
        expected_resolved,
        stdout,
        stderr
    );
}
