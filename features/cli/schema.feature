Feature: Schema
  Display the schema of a Parquet, Avro, or ORC file.

  Scenario: Schema Parquet default (csv output)
    When I run `datu schema fixtures/table.parquet`
    Then the command should succeed
    And the output should contain "one"
    And the output should contain "two"

  Scenario: Schema Avro default (csv output)
    When I run `datu schema fixtures/userdata5.avro`
    Then the command should succeed
    And the output should contain "id"
    And the output should contain "first_name"
    And the output should contain "email"

  Scenario: Schema Parquet with --output json
    When I run `datu schema fixtures/table.parquet --output json`
    Then the command should succeed
    And the output should be valid JSON
    And the output should contain "name"
    And the output should contain "data_type"
    And the output should contain "one"

  Scenario: Schema Avro with --output json
    When I run `datu schema fixtures/userdata5.avro -o json`
    Then the command should succeed
    And the output should be valid JSON
    And the output should contain "nullable"
    And the output should contain "id"

  Scenario: Schema Parquet with --output json-pretty
    When I run `datu schema fixtures/table.parquet --output json-pretty`
    Then the command should succeed
    And the output should be valid JSON
    And the output should contain "name"
    And the output should contain "data_type"
    And the output should contain "one"

  Scenario: Schema Avro with --output json-pretty
    When I run `datu schema fixtures/userdata5.avro -o json-pretty`
    Then the command should succeed
    And the output should be valid JSON
    And the output should contain "nullable"
    And the output should contain "id"

  Scenario: Schema Parquet with --output yaml
    When I run `datu schema fixtures/table.parquet --output yaml`
    Then the command should succeed
    And the output should be valid YAML
    And the output should contain "name"
    And the output should contain "one"

  Scenario: Schema Avro with --output yaml
    When I run `datu schema fixtures/userdata5.avro -o yaml`
    Then the command should succeed
    And the output should be valid YAML
    And the output should contain "email"

  Scenario: Schema ORC default (csv output)
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu schema $TEMPDIR/userdata5.orc`
    Then the command should succeed
    And the output should contain "id"
    And the output should contain "first_name"
