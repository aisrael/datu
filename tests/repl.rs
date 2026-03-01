use std::time::Duration;

use cucumber::World;
use cucumber::then;
use cucumber::when;
use expectrl::Expect;
use expectrl::session::OsSession;
use gherkin::Step;

#[derive(Debug, Default, World)]
pub struct ReplWorld {
    session: Option<OsSession>,
}

#[when(regex = r#"^I run the REPL$"#)]
fn run_datu_repl(world: &mut ReplWorld) {
    let datu_path = std::env::var("CARGO_BIN_EXE_datu")
        .expect("Environment variable 'CARGO_BIN_EXE_datu' not defined");
    let mut session = expectrl::spawn(datu_path).expect("Failed to spawn REPL");
    session.set_expect_timeout(Some(Duration::from_secs(5)));
    world.session = Some(session);
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

fn main() {
    futures::executor::block_on(ReplWorld::run("features/repl"));
}
