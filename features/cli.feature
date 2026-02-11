Feature: CLI

  Scenario: Print version
    When I run `datu --version`
    Then the output should contain "datu 0.2.0"
