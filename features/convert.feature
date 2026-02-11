Feature: Convert
  Convert between Parquet, Avro, CSV, JSON, and XLSX file formats.

  Scenario: Parquet to Avro
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/table.avro`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table.avro"
    And the file "$TEMPDIR/table.avro" should exist

  Scenario: Avro to Parquet
    When I run `dtfu convert fixtures/userdata5.avro $TEMPDIR/userdata5.parquet`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5.parquet"
    And the file "$TEMPDIR/userdata5.parquet" should exist

  Scenario: Parquet to CSV
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/table.csv`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table.csv"
    And the file "$TEMPDIR/table.csv" should exist
    And the first line of that file should contain "one,two"
    And that file should have 4 lines

  Scenario: Avro to CSV
    When I run `dtfu convert fixtures/userdata5.avro $TEMPDIR/userdata5.csv`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5.csv"
    And the file "$TEMPDIR/userdata5.csv" should exist
    And the first line of that file should contain "id,first_name"
    And that file should have 1001 lines

  Scenario: Parquet to CSV with --select
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/table_select.csv --select two,four`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_select.csv"
    And the file "$TEMPDIR/table_select.csv" should exist
    And the first line of that file should contain "two,four"
    And that file should have 4 lines

  Scenario: Avro to CSV with --select
    When I run `dtfu convert fixtures/userdata5.avro $TEMPDIR/userdata5_select.csv --select id,first_name,email`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5_select.csv"
    And the file "$TEMPDIR/userdata5_select.csv" should exist
    And the first line of that file should contain "id,first_name,email"
    And that file should have 1001 lines

  Scenario: Parquet to Avro with --limit
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/table_limit.avro --limit 2`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_limit.avro"
    And the file "$TEMPDIR/table_limit.avro" should exist

  Scenario: Avro to CSV with --limit and --select
    When I run `dtfu convert fixtures/userdata5.avro $TEMPDIR/userdata5_limit_select.csv --limit 3 --select id,email`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5_limit_select.csv"
    And the file "$TEMPDIR/userdata5_limit_select.csv" should exist
    And the first line of that file should contain "id,email"
    And that file should have 4 lines

  Scenario: Parquet to JSON
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/table.json`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table.json"
    And the file "$TEMPDIR/table.json" should exist

  Scenario: Avro to JSON
    When I run `dtfu convert fixtures/userdata5.avro $TEMPDIR/userdata5.json`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5.json"
    And the file "$TEMPDIR/userdata5.json" should exist

  Scenario: Parquet to XLSX
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/table.xlsx`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table.xlsx"
    And the file "$TEMPDIR/table.xlsx" should exist

  Scenario: Avro to XLSX
    When I run `dtfu convert fixtures/userdata5.avro $TEMPDIR/userdata5.xlsx`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5.xlsx"
    And the file "$TEMPDIR/userdata5.xlsx" should exist

  Scenario: Parquet to XLSX with --select
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/table_select.xlsx --select two,four`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_select.xlsx"
    And the file "$TEMPDIR/table_select.xlsx" should exist

  Scenario: Parquet to XLSX with --limit
    When I run `dtfu convert fixtures/table.parquet $TEMPDIR/table_limit.xlsx --limit 2`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_limit.xlsx"
    And the file "$TEMPDIR/table_limit.xlsx" should exist
