Feature: CLI

  Scenario: Print version
    When I run `dtfu --version`
    Then the output should contain "dtfu 0.1.2"
