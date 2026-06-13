Feature: Sample
  Print or write N randomly sampled rows from a Parquet, Avro, CSV, JSON, or ORC file.

  Scenario: Sample Parquet to stdout (default 10 lines)
    When I run `datu sample fixtures/table.parquet`
    Then the command should succeed
    And the output should contain "one"

  Scenario: Sample Parquet to stdout with -n 2
    When I run `datu sample fixtures/table.parquet -n 2`
    Then the command should succeed
    And the output should have a header and 2 lines

  Scenario: Sample Parquet to Avro file
    Given a file "fixtures/table.parquet"
    When I run `datu sample fixtures/table.parquet $TEMPDIR/sample.avro -n 2`
    Then the command should succeed
    And the output should contain "Sampled fixtures/table.parquet to $TEMPDIR/sample.avro"
    And the file "$TEMPDIR/sample.avro" should exist
    And that file should have 2 records

  Scenario: Sample Parquet to CSV file
    Given a file "fixtures/table.parquet"
    When I run `datu sample fixtures/table.parquet $TEMPDIR/sample.csv -n 2`
    Then the command should succeed
    And the output should contain "Sampled fixtures/table.parquet to $TEMPDIR/sample.csv"
    And the file "$TEMPDIR/sample.csv" should exist
    And that file should have 3 lines

  Scenario: Sample Avro to Parquet file
    Given a file "fixtures/userdata5.avro"
    When I run `datu sample fixtures/userdata5.avro $TEMPDIR/sample.parquet -n 5`
    Then the command should succeed
    And the output should contain "Sampled fixtures/userdata5.avro to $TEMPDIR/sample.parquet"
    And the file "$TEMPDIR/sample.parquet" should exist
    And that file should be a valid Parquet file
    And that file should have 5 records

  Scenario: Sample with -O overrides output type
    Given a file "fixtures/table.parquet"
    When I run `datu sample fixtures/table.parquet $TEMPDIR/sample.out -n 2 -O json`
    Then the command should succeed
    And the output should contain "Sampled fixtures/table.parquet to $TEMPDIR/sample.out"
    And the file "$TEMPDIR/sample.out" should exist
    And the file "$TEMPDIR/sample.out" should be valid JSON
