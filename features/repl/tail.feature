Feature: Tail

  Scenario: Tail from Parquet
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> tail(2) |> write("$TEMPDIR/tail_parquet.csv")
      ```
    Then the file "$TEMPDIR/tail_parquet.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "one,two,three,four,five,__index_level_0__"
    And that file should have 3 lines
    And that file should contain "bar"
    And that file should contain "baz"

  Scenario: Tail a single row
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> tail(1) |> write("$TEMPDIR/tail_one.csv")
      ```
    Then the file "$TEMPDIR/tail_one.csv" should exist
    And that file should be a CSV file
    And that file should have 2 lines
    And that file should contain "baz"

  Scenario: Tail more rows than available
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> tail(100) |> write("$TEMPDIR/tail_all.csv")
      ```
    Then the file "$TEMPDIR/tail_all.csv" should exist
    And that file should be a CSV file
    And that file should have 4 lines

  Scenario: Tail from Avro
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> tail(5) |> write("$TEMPDIR/tail_avro.csv")
      ```
    Then the file "$TEMPDIR/tail_avro.csv" should exist
    And that file should be a CSV file
    And the first line of that file should contain "id"
    And the first line of that file should contain "first_name"
    And that file should have 6 lines

  Scenario: Tail from ORC
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.orc") |> tail(3) |> write("$TEMPDIR/tail_orc.csv")
      ```
    Then the file "$TEMPDIR/tail_orc.csv" should exist
    And that file should be a CSV file
    And that file should have 4 lines

  Scenario: Tail with select
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:two, :three) |> tail(2) |> write("$TEMPDIR/tail_select.csv")
      ```
    Then the file "$TEMPDIR/tail_select.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "two,three"
    And that file should have 3 lines

  Scenario: Tail to JSON
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> tail(2) |> write("$TEMPDIR/tail.json")
      ```
    Then the file "$TEMPDIR/tail.json" should exist
    And that file should be valid JSON
    And that file should contain "bar"
    And that file should contain "baz"

  Scenario: Tail to YAML
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> tail(2) |> write("$TEMPDIR/tail.yaml")
      ```
    Then the file "$TEMPDIR/tail.yaml" should exist
    And that file should be valid YAML
    And that file should contain "two:"
    And that file should contain "baz"

  Scenario: Tail to Parquet
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> tail(10) |> write("$TEMPDIR/tail.parquet")
      ```
    Then the file "$TEMPDIR/tail.parquet" should exist
    And that file should be valid Parquet

  Scenario: Tail to Avro
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> tail(2) |> write("$TEMPDIR/tail.avro")
      ```
    Then the file "$TEMPDIR/tail.avro" should exist
    And that file should be valid Avro

  Scenario: Tail with select from Avro
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> select(:id, :email) |> tail(3) |> write("$TEMPDIR/tail_select_avro.csv")
      ```
    Then the file "$TEMPDIR/tail_select_avro.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "id,email"
    And that file should have 4 lines

  Scenario: Tail with select to JSON
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> select(:id, :first_name) |> tail(2) |> write("$TEMPDIR/tail_select.json")
      ```
    Then the file "$TEMPDIR/tail_select.json" should exist
    And that file should be valid JSON
    And that file should contain "id"
    And that file should contain "first_name"
