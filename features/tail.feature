Feature: Tail
  Print the last N rows of a Parquet or Avro file as CSV.

  Scenario: Tail Parquet default (10 lines)
    When I run `dtfu tail fixtures/table.parquet`
    Then the command should succeed
    And the first line should contain "one"

  Scenario: Tail Parquet with -n 2
    When I run `dtfu tail fixtures/table.parquet -n 2`
    Then the command should succeed
    And the first line should contain "one"
    And the first line should contain "two"

  Scenario: Tail Avro default (10 lines)
    When I run `dtfu tail fixtures/userdata5.avro`
    Then the command should succeed
    And the first line should contain "id"
    And the first line should contain "first_name"

  Scenario: Tail Avro with -n 3
    When I run `dtfu tail fixtures/userdata5.avro -n 3`
    Then the command should succeed
    And the first line should contain "email"

  Scenario: Tail Parquet with --select
    When I run `dtfu tail fixtures/table.parquet -n 2 --select two,four`
    Then the command should succeed
    And the first line should contain "two"
    And the first line should contain "four"

  Scenario: Tail Avro with --select
    When I run `dtfu tail fixtures/userdata5.avro -n 2 --select id,email`
    Then the command should succeed
    And the first line should contain "id"
    And the first line should contain "email"

  Scenario: Tail Parquet with --output csv
    When I run `dtfu tail fixtures/table.parquet -n 2 --output csv`
    Then the command should succeed
    And the first line should contain "one"
    And the first line should contain "two"

  Scenario: Tail Parquet with --output json
    When I run `dtfu tail fixtures/table.parquet -n 2 --output json`
    Then the command should succeed
    And the output should contain "["
    And the output should contain "one"

  Scenario: Tail Avro with --output csv
    When I run `dtfu tail fixtures/userdata5.avro -n 2 --output csv`
    Then the command should succeed
    And the first line should contain "id"
    And the first line should contain "first_name"

  Scenario: Tail Avro with --output json
    When I run `dtfu tail fixtures/userdata5.avro -n 2 --output json`
    Then the command should succeed
    And the output should contain "["
    And the output should contain "email"

  Scenario: Tail Parquet with --output yaml
    When I run `dtfu tail fixtures/table.parquet -n 2 --output yaml`
    Then the command should succeed
    And the output should contain "one"
    And the output should contain "two"

  Scenario: Tail Avro with --output yaml
    When I run `dtfu tail fixtures/userdata5.avro -n 2 --output yaml`
    Then the command should succeed
    And the output should contain "id"
    And the output should contain "email"
