use std::io::BufRead;
use std::io::Write;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use cucumber::World;
use cucumber::then;
use cucumber::when;
use expectrl::Expect;
use expectrl::session::OsSession;
use gherkin::Step;

const TEMPDIR_PLACEHOLDER: &str = "$TEMPDIR";

#[derive(Debug, Default, World)]
pub struct ReplWorld {
    session: Option<OsSession>,
    temp_dir: Option<tempfile::TempDir>,
    last_file: Option<String>,
}

fn replace_tempdir(s: &str, temp_path: &str) -> String {
    s.replace(TEMPDIR_PLACEHOLDER, temp_path)
}

fn resolve_path(world: &ReplWorld, path: &str) -> String {
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

fn ensure_temp_dir(world: &mut ReplWorld) -> String {
    if world.temp_dir.is_none() {
        world.temp_dir = Some(tempfile::tempdir().expect("Failed to create temp dir"));
    }
    world
        .temp_dir
        .as_ref()
        .unwrap()
        .path()
        .to_str()
        .expect("Temp path is not valid UTF-8")
        .to_string()
}

#[when(regex = r#"^datu is ran without a command$"#)]
fn run_datu_repl(world: &mut ReplWorld) {
    let datu_path = std::env::var("CARGO_BIN_EXE_datu")
        .expect("Environment variable 'CARGO_BIN_EXE_datu' not defined");
    let mut session = expectrl::spawn(datu_path).expect("Failed to spawn REPL");
    session.set_expect_timeout(Some(Duration::from_secs(5)));
    world.session = Some(session);
}

#[when(regex = r#"^the REPL is ran and the user types:$"#)]
fn repl_is_ran_and_user_types(world: &mut ReplWorld, step: &Step) {
    let input = step.docstring.as_ref().expect("Step requires a docstring");
    let temp_path = ensure_temp_dir(world);

    let datu_path = std::env::var("CARGO_BIN_EXE_datu")
        .expect("Environment variable 'CARGO_BIN_EXE_datu' not defined");

    let mut child = std::process::Command::new(datu_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn datu REPL");

    {
        let stdin = child.stdin.as_mut().expect("Failed to open stdin");
        for line in input.trim().lines() {
            let resolved = replace_tempdir(line.trim(), &temp_path);
            writeln!(stdin, "{resolved}").expect("Failed to write to REPL stdin");
        }
    }
    drop(child.stdin.take());

    let output = child.wait_with_output().expect("Failed to wait for datu");
    assert!(
        output.status.success(),
        "datu REPL exited with error.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[then(regex = r#"^the output should be:$"#)]
fn the_output_should_be(world: &mut ReplWorld, step: &Step) {
    let expected = step.docstring.as_ref().expect("Step requires a docstring");
    let session = world.session.as_mut().expect("No session running");
    let expected_trimmed = expected.trim();
    session
        .expect(expected_trimmed)
        .unwrap_or_else(|e| panic!("Expected to find '{expected_trimmed}' in output: {e}"));
}

#[then(regex = r#"^the file "(.+)" should exist$"#)]
fn file_should_exist(world: &mut ReplWorld, path: String) {
    let path_resolved = resolve_path(world, &path);
    assert!(
        Path::new(&path_resolved).exists(),
        "Expected file to exist: {path_resolved}"
    );
    world.last_file = Some(path_resolved);
}

#[then(regex = r#"^that file should be a CSV file$"#)]
fn that_file_should_be_csv(world: &mut ReplWorld) {
    let path = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let mut reader = csv::Reader::from_path(path).expect("Failed to open file as CSV");
    let headers = reader.headers().expect("Failed to read CSV headers");
    assert!(
        !headers.is_empty(),
        "Expected CSV file to have headers, but got none"
    );
}

#[then(regex = r#"^the first line of that file should be: "(.+)"$"#)]
fn first_line_of_file_should_be(world: &mut ReplWorld, expected: String) {
    let path = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let file = std::fs::File::open(path).expect("Failed to open file");
    let first_line = std::io::BufReader::new(file)
        .lines()
        .next()
        .expect("File is empty")
        .expect("Failed to read line");
    assert!(
        first_line == expected,
        "Expected first line to be '{expected}', but got: {first_line}"
    );
}

#[then(regex = r#"^the first line of that file should contain "(.+)"$"#)]
fn first_line_of_file_should_contain(world: &mut ReplWorld, expected: String) {
    let path = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let file = std::fs::File::open(path).expect("Failed to open file");
    let first_line = std::io::BufReader::new(file)
        .lines()
        .next()
        .expect("File is empty")
        .expect("Failed to read line");
    assert!(
        first_line.contains(&expected),
        "Expected first line to contain '{expected}', but got: {first_line}"
    );
}

#[then(regex = r#"^that file should be valid JSON$"#)]
fn that_file_should_be_valid_json(world: &mut ReplWorld) {
    let path = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let content = std::fs::read_to_string(path).expect("Failed to read file");
    serde_json::from_str::<serde_json::Value>(content.trim())
        .expect("Expected file to contain valid JSON, but parsing failed");
}

#[then(regex = r#"^that file should be valid YAML$"#)]
fn that_file_should_be_valid_yaml(world: &mut ReplWorld) {
    let path = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let content = std::fs::read_to_string(path).expect("Failed to read file");
    serde_yaml::from_str::<serde_yaml::Value>(content.trim())
        .expect("Expected file to contain valid YAML, but parsing failed");
}

#[then(regex = r#"^that file should contain "(.+)"$"#)]
fn that_file_should_contain(world: &mut ReplWorld, expected: String) {
    let path = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let content = std::fs::read_to_string(path).expect("Failed to read file");
    assert!(
        content.contains(&expected),
        "Expected file {path} to contain '{expected}', but it did not"
    );
}

#[then(regex = r#"^that file should have (\d+) lines$"#)]
fn that_file_should_have_n_lines(world: &mut ReplWorld, n: usize) {
    let path = world
        .last_file
        .as_ref()
        .expect("No file has been set; use 'the file \"...\" should exist' first");
    let file = std::fs::File::open(path).expect("Failed to open file");
    let line_count = std::io::BufReader::new(file)
        .lines()
        .filter(|r| r.as_ref().is_ok_and(|s| !s.trim().is_empty()))
        .count();
    assert!(
        line_count == n,
        "Expected file {path} to have {n} lines, but got {line_count}"
    );
}

fn main() {
    futures::executor::block_on(ReplWorld::run("features/repl"));
}
