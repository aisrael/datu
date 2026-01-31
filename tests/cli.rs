use std::process::Command;

use cucumber::World;
use cucumber::then;
use cucumber::when;

#[derive(Debug, Default, World)]
pub struct CliWorld {
    output: Option<std::process::Output>,
}

#[when(regex = r#"^I run `dtfu (.+)`$"#)]
fn run_dtfu_with_args(world: &mut CliWorld, args: String) {
    let args: Vec<&str> = args.split_whitespace().collect();
    let output = Command::new(env!("CARGO_BIN_EXE_dtfu"))
        .args(&args)
        .output()
        .expect("Failed to execute dtfu");
    world.output = Some(output);
}

#[then(regex = r#"^the output should contain "(.+)"$"#)]
fn output_should_contain(world: &mut CliWorld, expected: String) {
    let output = world.output.as_ref().expect("No output captured");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.contains(&expected),
        "Expected output to contain '{}', but got:\nstdout: {}\nstderr: {}",
        expected,
        stdout,
        stderr
    );
}

fn main() {
    futures::executor::block_on(CliWorld::run("features/cli.feature"));
}
