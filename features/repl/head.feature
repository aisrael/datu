Feature: Head

  Scenario: Head from Parquet
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> head(2) |> write("$TEMPDIR/head_parquet.csv")
      ```
    Then the file "$TEMPDIR/head_parquet.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "one,two,three,four,five,__index_level_0__"
    And that file should have 3 lines
    And that file should contain "foo"
    And that file should contain "bar"

  Scenario: Head a single row
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> head(1) |> write("$TEMPDIR/head_one.csv")
      ```
    Then the file "$TEMPDIR/head_one.csv" should exist
    And that file should be a CSV file
    And that file should have 2 lines
    And that file should contain "foo"

  Scenario: Head more rows than available
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> head(100) |> write("$TEMPDIR/head_all.csv")
      ```
    Then the file "$TEMPDIR/head_all.csv" should exist
    And that file should be a CSV file
    And that file should have 4 lines

  Scenario: Head from Avro
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> head(5) |> write("$TEMPDIR/head_avro.csv")
      ```
    Then the file "$TEMPDIR/head_avro.csv" should exist
    And that file should be a CSV file
    And the first line of that file should contain "id"
    And the first line of that file should contain "first_name"
    And that file should have 6 lines

  Scenario: Head from ORC
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.orc") |> head(3) |> write("$TEMPDIR/head_orc.csv")
      ```
    Then the file "$TEMPDIR/head_orc.csv" should exist
    And that file should be a CSV file
    And that file should have 4 lines

  Scenario: Head with select
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:two, :three) |> head(2) |> write("$TEMPDIR/head_select.csv")
      ```
    Then the file "$TEMPDIR/head_select.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "two,three"
    And that file should have 3 lines

  Scenario: Head to JSON
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> head(2) |> write("$TEMPDIR/head.json")
      ```
    Then the file "$TEMPDIR/head.json" should exist
    And that file should be valid JSON
    And that file should contain "foo"
    And that file should contain "bar"

  Scenario: Head to YAML
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> head(2) |> write("$TEMPDIR/head.yaml")
      ```
    Then the file "$TEMPDIR/head.yaml" should exist
    And that file should be valid YAML
    And that file should contain "two:"
    And that file should contain "foo"

  Scenario: Head to Parquet
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> head(10) |> write("$TEMPDIR/head.parquet")
      ```
    Then the file "$TEMPDIR/head.parquet" should exist
    And that file should be valid Parquet

  Scenario: Head to Avro
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> head(2) |> write("$TEMPDIR/head.avro")
      ```
    Then the file "$TEMPDIR/head.avro" should exist
    And that file should be valid Avro

  Scenario: Head with select from Avro
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> select(:id, :email) |> head(3) |> write("$TEMPDIR/head_select_avro.csv")
      ```
    Then the file "$TEMPDIR/head_select_avro.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "id,email"
    And that file should have 4 lines

  Scenario: Head with select to JSON
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> select(:id, :first_name) |> head(2) |> write("$TEMPDIR/head_select.json")
      ```
    Then the file "$TEMPDIR/head_select.json" should exist
    And that file should be valid JSON
    And that file should contain "id"
    And that file should contain "first_name"
