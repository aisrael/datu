Feature: Sample

  Scenario: Sample from Parquet
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> sample(2) |> write("$TEMPDIR/sample_parquet.csv")
      ```
    Then the file "$TEMPDIR/sample_parquet.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "one,two,three,four,five,__index_level_0__"
    And that file should have 3 lines

  Scenario: Sample with a keyword count
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> sample(n: 1) |> write("$TEMPDIR/sample_keyword.csv")
      ```
    Then the file "$TEMPDIR/sample_keyword.csv" should exist
    And that file should have 2 lines

  Scenario: Sample defaults to 10 rows
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.parquet") |> sample() |> write("$TEMPDIR/sample_default.csv")
      ```
    Then the file "$TEMPDIR/sample_default.csv" should exist
    And that file should have 11 lines
