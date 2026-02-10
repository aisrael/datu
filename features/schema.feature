Feature: Schema

  Display the schema of a Parquet or Avro file.

  Scenario: Schema Parquet default (csv output)
    When I run `dtfu schema fixtures/table.parquet`
    Then the command should succeed
    And the output should contain "one"
    And the output should contain "two"

  Scenario: Schema Avro default (csv output)
    When I run `dtfu schema fixtures/userdata5.avro`
    Then the command should succeed
    And the output should contain "id"
    And the output should contain "first_name"
    And the output should contain "email"

  Scenario: Schema Parquet with --output json
    When I run `dtfu schema fixtures/table.parquet --output json`
    Then the command should succeed
    And the output should contain "name"
    And the output should contain "data_type"
    And the output should contain "one"

  Scenario: Schema Avro with --output json
    When I run `dtfu schema fixtures/userdata5.avro -o json`
    Then the command should succeed
    And the output should contain "nullable"
    And the output should contain "id"

  Scenario: Schema Parquet with --output yaml
    When I run `dtfu schema fixtures/table.parquet --output yaml`
    Then the command should succeed
    And the output should contain "name"
    And the output should contain "one"

  Scenario: Schema Avro with --output yaml
    When I run `dtfu schema fixtures/userdata5.avro -o yaml`
    Then the command should succeed
    And the output should contain "email"
