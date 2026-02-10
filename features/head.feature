Feature: Head
  Print the first N rows of a Parquet or Avro file as CSV.

  Scenario: Head Parquet default (10 lines)
    When I run `dtfu head fixtures/userdata.parquet`
    Then the command should succeed
    And the output should have a header and 10 lines

  Scenario: Head Parquet with -n 2
    When I run `dtfu head fixtures/userdata.parquet -n 2`
    Then the command should succeed
    And the output should have a header and 2 lines

  Scenario: Head Avro default (10 lines)
    When I run `dtfu head fixtures/userdata5.avro`
    Then the command should succeed
    And the first line should contain "id"
    And the first line should contain "first_name"

  Scenario: Head Avro with -n 3
    When I run `dtfu head fixtures/userdata5.avro -n 3`
    Then the command should succeed
    And the first line should contain "email"

  Scenario: Head Parquet with --select
    When I run `dtfu head fixtures/userdata.parquet -n 2 --select id,last_name`
    Then the command should succeed
    And the first line should contain "id"
    And the first line should contain "last_name"

  Scenario: Head Avro with --select
    When I run `dtfu head fixtures/userdata5.avro -n 2 --select id,email`
    Then the command should succeed
    And the output should have a header and 2 lines
    And the first line should contain "id"
    And the first line should contain "email"
