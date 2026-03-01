Feature: Count
  Return the number of rows in a Parquet, Avro, or ORC file.

  Scenario: Count Parquet
    When I run `datu count fixtures/table.parquet`
    Then the command should succeed
    And the output should contain "3"

  Scenario: Count Avro
    When I run `datu count fixtures/userdata5.avro`
    Then the command should succeed
    And the output should contain "1000"

  Scenario: Count ORC
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu count $TEMPDIR/userdata5.orc`
    Then the command should succeed
    And the output should contain "10"
