use std::io::BufRead;
use std::path::Path;
use std::process::Command;

use cucumber::World;
use cucumber::then;
use cucumber::when;
use datu::utils;
use gherkin::Step;

const TEMPDIR_PLACEHOLDER: &str = "$TEMPDIR";

#[derive(Debug, Default, World)]
pub struct CliWorld {
    output: Option<std::process::Output>,
    temp_dir: Option<tempfile::TempDir>,
    /// Last file path used in a "the file \"...\" should exist" step (resolved).
    last_file: Option<String>,
}

fn replace_tempdir(s: &str, temp_path: &str) -> String {
    s.replace(TEMPDIR_PLACEHOLDER, temp_path)
}

#[when(regex = r#"^I run `datu (.+)`$"#)]
fn run_datu_with_args(world: &mut CliWorld, args: String) {
    let args_str = args;
    let temp_path = if args_str.contains(TEMPDIR_PLACEHOLDER) {
        Some(if let Some(ref temp_dir) = world.temp_dir {
            temp_dir
                .path()
                .to_str()
                .expect("Temp path is not valid UTF-8")
                .to_string()
        } else {
            let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
            let path = temp_dir
                .path()
                .to_str()
                .expect("Temp path is not valid UTF-8")
                .to_string();
            world.temp_dir = Some(temp_dir);
            path
        })
    } else {
        None
    };

    let resolved_args = if let Some(temp_path) = temp_path {
        replace_tempdir(&args_str, &temp_path)
    } else {
        args_str
    };

    let args: Vec<&str> = resolved_args.split_whitespace().collect();
    let datu_path = std::env::var("CARGO_BIN_EXE_datu")
        .expect("Environment variable 'CARGO_BIN_EXE_datu' not defined");
    let output = Command::new(datu_path)
        .args(&args)
        .output()
        .expect("Failed to execute datu");
    world.output = Some(output);
}

#[then(regex = r#"^the command should succeed$"#)]
fn command_should_succeed(world: &mut CliWorld) {
    let output = world.output.as_ref().expect("No output captured");
    assert!(
        output.status.success(),
        "Command failed with exit code {:?}:\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[then(regex = r#"^the first line should contain "(.+)"$"#)]
fn first_line_should_contain(world: &mut CliWorld, expected: String) {
    let output = world.output.as_ref().expect("No output captured");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let first_line = stdout.lines().next().unwrap_or("").trim();

    let expected_resolved = if let Some(ref temp_dir) = world.temp_dir {
        let temp_path = temp_dir
            .path()
            .to_str()
            .expect("Temp path is not valid UTF-8");
        replace_tempdir(&expected, temp_path)
    } else {
        expected
    };

    assert!(
        first_line.contains(&expected_resolved),
        "Expected first line to contain '{}', but got: {}",
        expected_resolved,
        first_line
    );
}

#[then(regex = r#"^the output should be:$"#)]
fn output_should_be_docstring(world: &mut CliWorld, step: &Step) {
    let expected = step
        .docstring
        .as_ref()
        .expect("Step requires a docstring (triple-quoted or ``` block)");
    let output = world.output.as_ref().expect("No output captured");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);
    let expected_trimmed = expected.trim();
    let output_trimmed = combined.trim();
    assert!(
        output_trimmed.contains(expected_trimmed),
        "Expected output to contain the given content, but it did not.\nExpected to find:\n---\n{}\n---\nActual output:\n---\n{}\n---",
        expected_trimmed,
        output_trimmed
    );
}

#[then(regex = r#"^the output should contain "(.+)"$"#)]
fn output_should_contain(world: &mut CliWorld, expected: String) {
    let output = world.output.as_ref().expect("No output captured");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    let expected_resolved = if let Some(ref temp_dir) = world.temp_dir {
        let temp_path = temp_dir
            .path()
            .to_str()
            .expect("Temp path is not valid UTF-8");
        replace_tempdir(&expected, temp_path)
    } else {
        expected
    };

    assert!(
        combined.contains(&expected_resolved),
        "Expected output to contain '{}', but got:\nstdout: {}\nstderr: {}",
        expected_resolved,
        stdout,
        stderr
    );
}

#[then(regex = r#"^the output should be valid JSON$"#)]
fn output_should_be_valid_json(world: &mut CliWorld) {
    let output = world.output.as_ref().expect("No output captured");
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str::<serde_json::Value>(stdout.trim())
        .expect("Expected output to be valid JSON, but parsing failed");
}

#[then(regex = r#"^the output should be valid YAML$"#)]
fn output_should_be_valid_yaml(world: &mut CliWorld) {
    let output = world.output.as_ref().expect("No output captured");
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_yaml::from_str::<serde_yaml::Value>(stdout.trim())
        .expect("Expected output to be valid YAML, but parsing failed");
}

#[then(regex = r#"^the output should have a header and (\d+) lines$"#)]
fn output_should_have_header_and_n_lines(world: &mut CliWorld, n: usize) {
    let output = world.output.as_ref().expect("No output captured");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    assert!(
        !lines.is_empty(),
        "Expected output to have a header line, but got no lines"
    );
    let data_lines = lines.len() - 1;
    assert!(
        data_lines == n,
        "Expected {n} data lines (plus header), but got {} lines total ({data_lines} data lines)",
        lines.len()
    );
}

#[then(regex = r#"^the file "(.+)" should exist$"#)]
fn file_should_exist(world: &mut CliWorld, path: String) {
    let path_resolved = if let Some(ref temp_dir) = world.temp_dir {
        let temp_path = temp_dir
            .path()
            .to_str()
            .expect("Temp path is not valid UTF-8");
        replace_tempdir(&path, temp_path)
    } else {
        path.clone()
    };

    assert!(
        Path::new(&path_resolved).exists(),
        "Expected file to exist: {}",
        path_resolved
    );
    world.last_file = Some(path_resolved);
}

fn resolve_path(world: &CliWorld, path: &str) -> String {
    if let Some(ref temp_dir) = world.temp_dir {
        let temp_path = temp_dir
            .path()
            .to_str()
            .expect("Temp path is not valid UTF-8");
        replace_tempdir(path, temp_path)
    } else {
        path.to_string()
    }
}

#[then(regex = r#"^the file "(.+)" should have a first line containing "(.+)"$"#)]
fn file_should_have_first_line_containing(world: &mut CliWorld, path: String, expected: String) {
    let path_resolved = resolve_path(world, &path);
    let file = std::fs::File::open(&path_resolved).expect("Failed to open file");
    let first_line = std::io::BufReader::new(file)
        .lines()
        .next()
        .expect("File is empty")
        .expect("Failed to read line");
    assert!(
        first_line.contains(&expected),
        "Expected first line of {} to contain '{}', but got: {}",
        path_resolved,
        expected,
        first_line
    );
}

#[then(regex = r#"^the file "(.+)" should have (\d+) lines$"#)]
fn file_should_have_n_lines(world: &mut CliWorld, path: String, n: usize) {
    let path_resolved = resolve_path(world, &path);
    let file = std::fs::File::open(&path_resolved).expect("Failed to open file");
    let line_count = std::io::BufReader::new(file)
        .lines()
        .filter(|r| r.as_ref().is_ok_and(|s| !s.trim().is_empty()))
        .count();
    assert!(
        line_count == n,
        "Expected file {} to have {} lines, but got {}",
        path_resolved,
        n,
        line_count
    );
}

#[then(regex = r#"^the first line of that file should contain "(.+)"$"#)]
fn first_line_of_that_file_should_contain(world: &mut CliWorld, expected: String) {
    let path_resolved = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let file = std::fs::File::open(path_resolved).expect("Failed to open file");
    let first_line = std::io::BufReader::new(file)
        .lines()
        .next()
        .expect("File is empty")
        .expect("Failed to read line");
    assert!(
        first_line.contains(&expected),
        "Expected first line of {} to contain '{}', but got: {}",
        path_resolved,
        expected,
        first_line
    );
}

#[then(regex = r#"^that file should contain "(.+)"$"#)]
fn that_file_should_contain(world: &mut CliWorld, expected: String) {
    let path_resolved = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let content = std::fs::read_to_string(path_resolved).expect("Failed to read file");
    assert!(
        content.contains(&expected),
        "Expected file {} to contain '{}', but it did not",
        path_resolved,
        expected
    );
}

#[then(regex = r#"^that file should contain `(.+)`$"#)]
fn that_file_should_contain_literal(world: &mut CliWorld, expected: String) {
    let path_resolved = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let content = std::fs::read_to_string(path_resolved).expect("Failed to read file");
    let expected = utils::unescape_str(&expected).expect("Failed to unescape string: `{expected}`");
    assert!(
        content.contains(&expected),
        "Expected file {} to contain '{}', but it did not",
        path_resolved,
        expected
    );
}

#[then(regex = r#"^the file "(.+)" should be valid JSON$"#)]
fn file_should_be_valid_json(world: &mut CliWorld, path: String) {
    let path_resolved = resolve_path(world, &path);
    let content = std::fs::read_to_string(&path_resolved).expect("Failed to read file");
    serde_json::from_str::<serde_json::Value>(content.trim())
        .expect("Expected file to contain valid JSON, but parsing failed");
}

#[then(regex = r#"^the file "(.+)" should be valid YAML$"#)]
fn file_should_be_valid_yaml(world: &mut CliWorld, path: String) {
    let path_resolved = resolve_path(world, &path);
    let content = std::fs::read_to_string(&path_resolved).expect("Failed to read file");
    serde_yaml::from_str::<serde_yaml::Value>(content.trim())
        .expect("Expected file to contain valid YAML, but parsing failed");
}

#[then(regex = r#"^that file should be valid JSON$"#)]
fn that_file_should_be_valid_json(world: &mut CliWorld) {
    let path_resolved = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let content = std::fs::read_to_string(path_resolved).expect("Failed to read file");
    serde_json::from_str::<serde_json::Value>(content.trim())
        .expect("Expected file to contain valid JSON, but parsing failed");
}

#[then(regex = r#"^that file should be valid YAML$"#)]
fn that_file_should_be_valid_yaml(world: &mut CliWorld) {
    let path_resolved = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let content = std::fs::read_to_string(path_resolved).expect("Failed to read file");
    serde_yaml::from_str::<serde_yaml::Value>(content.trim())
        .expect("Expected file to contain valid YAML, but parsing failed");
}

#[then(regex = r#"^the file "(.+)" should contain:$"#)]
fn file_should_contain_docstring(world: &mut CliWorld, path: String, step: &Step) {
    let expected = step
        .docstring
        .as_ref()
        .expect("Step requires a docstring (triple-quoted or ``` block)");
    let path_resolved = resolve_path(world, &path);
    let content = std::fs::read_to_string(&path_resolved).expect("Failed to read file");
    let expected_trimmed = expected.trim();
    let content_trimmed = content.trim();
    assert!(
        content_trimmed.contains(expected_trimmed),
        "Expected file {} to contain the given content, but it did not.\nExpected to find:\n---\n{}\n---\nActual content:\n---\n{}\n---",
        path_resolved,
        expected_trimmed,
        content_trimmed
    );
}

#[then(regex = r#"^that file should have (\d+) lines$"#)]
fn that_file_should_have_n_lines(world: &mut CliWorld, n: usize) {
    let path_resolved = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let file = std::fs::File::open(path_resolved).expect("Failed to open file");
    let line_count = std::io::BufReader::new(file)
        .lines()
        .filter(|r| r.as_ref().is_ok_and(|s| !s.trim().is_empty()))
        .count();
    assert!(
        line_count == n,
        "Expected file {} to have {} lines, but got {}",
        path_resolved,
        n,
        line_count
    );
}

fn main() {
    futures::executor::block_on(CliWorld::run("features"));
}
