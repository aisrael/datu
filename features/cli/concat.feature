Feature: Concat
  Concatenate two or more input files into a single output file. Input and output formats are
  inferred from file extensions, or can be specified explicitly with --input and --output.

  Scenario: Concat two explicit Avro files into Parquet
    Given a file "fixtures/concat_part1.avro"
    Given a file "fixtures/concat_part2.avro"
    When I run `datu concat fixtures/concat_part1.avro fixtures/concat_part2.avro $TEMPDIR/all.parquet`
    Then the command should succeed
    And the output should contain "Concatenated 2 files into $TEMPDIR/all.parquet"
    And the file "$TEMPDIR/all.parquet" should exist
    And that file should be a valid Parquet file
    And that file should have 10 records

  Scenario: Concat using a glob pattern into Parquet
    Given a file "fixtures/concat_part1.avro"
    When I run `datu concat fixtures/concat_part*.avro $TEMPDIR/all_glob.parquet`
    Then the command should succeed
    And the output should contain "Concatenated 2 files into $TEMPDIR/all_glob.parquet"
    And the file "$TEMPDIR/all_glob.parquet" should exist
    And that file should be a valid Parquet file
    And that file should have 10 records

  Scenario: Concat a single input file
    Given a file "fixtures/concat_part1.avro"
    When I run `datu concat fixtures/concat_part1.avro $TEMPDIR/single.parquet`
    Then the command should succeed
    And the output should contain "Concatenated 1 file into $TEMPDIR/single.parquet"
    And the file "$TEMPDIR/single.parquet" should exist
    And that file should have 5 records

  Scenario: Concat to CSV
    Given a file "fixtures/concat_part1.avro"
    When I run `datu concat fixtures/concat_part1.avro fixtures/concat_part2.avro $TEMPDIR/all.csv`
    Then the command should succeed
    And the file "$TEMPDIR/all.csv" should exist
    And the first line of that file should contain "id,first_name"
    And that file should have 11 lines

  Scenario: Concat requires at least two positional arguments
    Given a file "fixtures/concat_part1.avro"
    When I run `datu concat fixtures/concat_part1.avro`
    Then the command should fail

  Scenario: Concat fails when a glob pattern matches no files
    When I run `datu concat fixtures/no_such_part*.avro $TEMPDIR/empty.parquet`
    Then the command should fail
    And the output should contain "matched no files"

  Scenario: Concat two Avro files with --output-avro-compression snappy
    Given a file "fixtures/concat_part1.avro"
    Given a file "fixtures/concat_part2.avro"
    When I run `datu concat fixtures/concat_part1.avro fixtures/concat_part2.avro $TEMPDIR/all_snappy.avro --output-avro-compression snappy`
    Then the command should succeed
    And the file "$TEMPDIR/all_snappy.avro" should exist
    And the file "$TEMPDIR/all_snappy.avro" should be a valid Avro file with codec "snappy"

  Scenario: Concat two Avro files into Parquet with --output-parquet-compression gzip
    Given a file "fixtures/concat_part1.avro"
    Given a file "fixtures/concat_part2.avro"
    When I run `datu concat fixtures/concat_part1.avro fixtures/concat_part2.avro $TEMPDIR/all_gzip.parquet --output-parquet-compression gzip`
    Then the command should succeed
    And the file "$TEMPDIR/all_gzip.parquet" should exist
    And the file "$TEMPDIR/all_gzip.parquet" should be a valid Parquet file with codec "gzip"

  Scenario: Concat fails on incompatible schemas
    Given a file "fixtures/file1.avro"
    Given a file "fixtures/file2.avro"
    When I run `datu concat fixtures/file1.avro fixtures/file2.avro $TEMPDIR/mismatch.parquet`
    Then the command should fail
