Feature: Tail
  Print the last N rows of a Parquet, Avro, or ORC file as CSV.

  Scenario: Tail Parquet default (10 lines)
    When I run `datu tail fixtures/table.parquet`
    Then the command should succeed
    And the first line should contain "one"

  Scenario: Tail Parquet with -n 2
    When I run `datu tail fixtures/table.parquet -n 2`
    Then the command should succeed
    And the first line should contain "one"
    And the first line should contain "two"

  Scenario: Tail Avro default (10 lines)
    When I run `datu tail fixtures/userdata5.avro`
    Then the command should succeed
    And the first line should contain "id"
    And the first line should contain "first_name"

  Scenario: Tail Avro with -n 3
    When I run `datu tail fixtures/userdata5.avro -n 3`
    Then the command should succeed
    And the first line should contain "email"

  Scenario: Tail Parquet with --select
    When I run `datu tail fixtures/table.parquet -n 2 --select two,four`
    Then the command should succeed
    And the first line should contain "two"
    And the first line should contain "four"

  Scenario: Tail Avro with --select
    When I run `datu tail fixtures/userdata5.avro -n 2 --select id,email`
    Then the command should succeed
    And the first line should contain "id"
    And the first line should contain "email"

  Scenario: Tail ORC default (10 lines)
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu tail $TEMPDIR/userdata5.orc`
    Then the command should succeed
    And the first line should contain "id"
    And the first line should contain "first_name"

  Scenario: Tail ORC with -n 3
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu tail $TEMPDIR/userdata5.orc -n 3`
    Then the command should succeed
    And the first line should contain "id"

  Scenario: Tail ORC with --select
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu tail $TEMPDIR/userdata5.orc -n 2 --select id,first_name`
    Then the command should succeed
    And the first line should contain "id"
    And the first line should contain "first_name"

  Scenario: Tail ORC with --output csv
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu tail $TEMPDIR/userdata5.orc -n 2 --output csv`
    Then the command should succeed
    And the first line should contain "id"
    And the first line should contain "first_name"

  Scenario: Tail ORC with --output json
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu tail $TEMPDIR/userdata5.orc -n 2 --output json`
    Then the command should succeed
    And the output should contain "["
    And the output should contain "id"

  Scenario: Tail ORC with --output yaml
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu tail $TEMPDIR/userdata5.orc -n 2 --output yaml`
    Then the command should succeed
    And the output should contain "id"
    And the output should contain "first_name"

  Scenario: Tail Parquet with --output csv
    When I run `datu tail fixtures/table.parquet -n 2 --output csv`
    Then the command should succeed
    And the first line should contain "one"
    And the first line should contain "two"

  Scenario: Tail Parquet with --output json
    When I run `datu tail fixtures/table.parquet -n 2 --output json`
    Then the command should succeed
    And the output should contain "["
    And the output should contain "one"

  Scenario: Tail Avro with --output csv
    When I run `datu tail fixtures/userdata5.avro -n 2 --output csv`
    Then the command should succeed
    And the first line should contain "id"
    And the first line should contain "first_name"

  Scenario: Tail Avro with --output json
    When I run `datu tail fixtures/userdata5.avro -n 2 --output json`
    Then the command should succeed
    And the output should contain "["
    And the output should contain "email"

  Scenario: Tail Parquet with --output yaml
    When I run `datu tail fixtures/table.parquet -n 2 --output yaml`
    Then the command should succeed
    And the output should contain "one"
    And the output should contain "two"

  Scenario: Tail Avro with --output yaml
    When I run `datu tail fixtures/userdata5.avro -n 2 --output yaml`
    Then the command should succeed
    And the output should contain "id"
    And the output should contain "email"
