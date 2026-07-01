Feature: Split
  Split a single input file into multiple output files of at most N rows (or, approximately, N
  bytes) each. Input and output formats are inferred from file extensions, or can be specified
  explicitly with --input and --output. Partition files are named by inserting a zero-padded
  ".partNNNNN" segment before the extension of the (optional) output path, which defaults to the
  input path.

  Scenario: Split Parquet into partitions of 2 rows
    Given a file "fixtures/table.parquet"
    When I run `datu split fixtures/table.parquet $TEMPDIR/out.parquet --split 2`
    Then the command should succeed
    And the output should contain "Split fixtures/table.parquet into 2 files (3 rows)"
    And the file "$TEMPDIR/out.part00001.parquet" should exist
    And that file should have 2 records
    And the file "$TEMPDIR/out.part00002.parquet" should exist
    And that file should have 1 records

  Scenario: Split Avro into partitions of 2 rows
    Given a file "fixtures/concat_part1.avro"
    When I run `datu split fixtures/concat_part1.avro $TEMPDIR/part.avro --split 2`
    Then the command should succeed
    And the output should contain "Split fixtures/concat_part1.avro into 3 files (5 rows)"
    And the file "$TEMPDIR/part.part00001.avro" should exist
    And that file should have 2 records
    And the file "$TEMPDIR/part.part00002.avro" should exist
    And that file should have 2 records
    And the file "$TEMPDIR/part.part00003.avro" should exist
    And that file should have 1 records

  Scenario: Split with --limit caps the total rows processed
    Given a file "fixtures/userdata5.avro"
    When I run `datu split fixtures/userdata5.avro $TEMPDIR/u.avro --split 100 --limit 250`
    Then the command should succeed
    And the output should contain "Split fixtures/userdata5.avro into 3 files (250 rows)"
    And the file "$TEMPDIR/u.part00001.avro" should exist
    And that file should have 100 records
    And the file "$TEMPDIR/u.part00003.avro" should exist
    And that file should have 50 records

  Scenario: Split Parquet into CSV partitions
    Given a file "fixtures/table.parquet"
    When I run `datu split fixtures/table.parquet $TEMPDIR/out.csv --split 2`
    Then the command should succeed
    And the file "$TEMPDIR/out.part00001.csv" should exist
    And the first line of that file should contain "one,two"
    And that file should have 3 lines
    And the file "$TEMPDIR/out.part00002.csv" should exist
    And that file should have 2 lines

  Scenario: Split with the default --split does not fragment a small file
    Given a file "fixtures/table.parquet"
    When I run `datu split fixtures/table.parquet $TEMPDIR/whole.parquet`
    Then the command should succeed
    And the output should contain "Split fixtures/table.parquet into 1 file (3 rows)"
    And the file "$TEMPDIR/whole.part00001.parquet" should exist
    And that file should have 3 records

  Scenario: Split fails when --split is 0
    Given a file "fixtures/table.parquet"
    When I run `datu split fixtures/table.parquet $TEMPDIR/out.parquet --split 0`
    Then the command should fail
    And the output should contain "must be greater than 0"

  Scenario: Split by a decimal byte size (kb) produces multiple partitions
    Given a file "fixtures/userdata5.avro"
    When I run `datu split fixtures/userdata5.avro $TEMPDIR/u.avro --split 20kb`
    Then the command should succeed
    And the output should contain "Split fixtures/userdata5.avro into"
    And the output should contain "rows)"
    And the file "$TEMPDIR/u.part00001.avro" should exist
    And the file "$TEMPDIR/u.part00002.avro" should exist

  Scenario: Split by size units is case-insensitive
    Given a file "fixtures/userdata5.avro"
    When I run `datu split fixtures/userdata5.avro $TEMPDIR/u.avro --split 10KB`
    Then the command should succeed
    And the file "$TEMPDIR/u.part00001.avro" should exist
    And the file "$TEMPDIR/u.part00002.avro" should exist

  Scenario: Split by a binary byte size (MiB) succeeds
    Given a file "fixtures/userdata5.avro"
    When I run `datu split fixtures/userdata5.avro $TEMPDIR/u.avro --split 1MiB`
    Then the command should succeed
    And the output should contain "Split fixtures/userdata5.avro into 1 file (1000 rows)"
    And the file "$TEMPDIR/u.part00001.avro" should exist

  Scenario: Split fails on an unknown size unit
    Given a file "fixtures/table.parquet"
    When I run `datu split fixtures/table.parquet $TEMPDIR/out.parquet --split 10xb`
    Then the command should fail
    And the output should contain "unknown size unit"

  Scenario: Split fails when a byte size of 0 is given
    Given a file "fixtures/table.parquet"
    When I run `datu split fixtures/table.parquet $TEMPDIR/out.parquet --split 0mb`
    Then the command should fail
    And the output should contain "must be greater than 0"
