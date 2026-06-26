Feature: Diff
  Compare two data files of the same format row-by-row.

  Scenario: Identical Avro files
    Given a file "fixtures/file1.avro"
    When I run `datu diff fixtures/file1.avro fixtures/file1.avro`
    Then the command should succeed
    And the output should contain "Files are identical (4 rows)"

  Scenario: Avro files with schema and row differences
    Given a file "fixtures/file1.avro"
    And a file "fixtures/file2.avro"
    When I run `datu diff fixtures/file1.avro fixtures/file2.avro`
    Then the command should succeed
    And the output should contain "Schema differences"
    And the output should contain "Only in fixtures/file2.avro"
    And the output should contain "email: Utf8"
    And the output should contain "Only in fixtures/file1.avro (1 row)"
    And the output should contain "foo"
    And the output should contain "Only in fixtures/file2.avro (1 row)"
    And the output should contain "fizz"

  Scenario: Files with different formats exit with an error
    When I run `datu diff fixtures/file1.avro fixtures/table.csv`
    Then the command should fail
    And the output should contain "different formats"

  Scenario: Identical files with explicit --input override
    Given a file "fixtures/file1.avro"
    When I run `datu diff fixtures/file1.avro fixtures/file1.avro --input avro`
    Then the command should succeed
    And the output should contain "Files are identical (4 rows)"
