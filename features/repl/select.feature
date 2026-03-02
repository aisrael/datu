Feature: Select

  Scenario: Select columns using symbol syntax
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:two, :three) |> write("$TEMPDIR/select_symbols.csv")
      ```
    Then the file "$TEMPDIR/select_symbols.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "two,three"
    And that file should have 4 lines

  Scenario: Select columns using string syntax
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select("two", "three") |> write("$TEMPDIR/select_strings.csv")
      ```
    Then the file "$TEMPDIR/select_strings.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "two,three"
    And that file should have 4 lines

  Scenario: Select a single column
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:two) |> write("$TEMPDIR/select_single.csv")
      ```
    Then the file "$TEMPDIR/select_single.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "two"
    And that file should have 4 lines

  Scenario: Select reorders columns
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:three, :one) |> write("$TEMPDIR/select_reorder.csv")
      ```
    Then the file "$TEMPDIR/select_reorder.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "three,one"

  Scenario: Select many columns
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:five, :four, :three, :two, :one) |> write("$TEMPDIR/select_many.csv")
      ```
    Then the file "$TEMPDIR/select_many.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "five,four,three,two,one"
    And that file should have 4 lines

  Scenario: Select from Avro
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> select(:id, :email) |> write("$TEMPDIR/select_avro.csv")
      ```
    Then the file "$TEMPDIR/select_avro.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "id,email"

  Scenario: Select from ORC
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.orc") |> select(:_col1, :_col2) |> write("$TEMPDIR/select_orc.csv")
      ```
    Then the file "$TEMPDIR/select_orc.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "_col1,_col2"

  Scenario: Select with head
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> select(:id, :first_name) |> head(5) |> write("$TEMPDIR/select_head.csv")
      ```
    Then the file "$TEMPDIR/select_head.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "id,first_name"
    And that file should have 6 lines

  Scenario: Select with tail
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:two, :one) |> tail(1) |> write("$TEMPDIR/select_tail.csv")
      ```
    Then the file "$TEMPDIR/select_tail.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "two,one"
    And that file should have 2 lines

  Scenario: Select to JSON
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:two, :three) |> write("$TEMPDIR/select.json")
      ```
    Then the file "$TEMPDIR/select.json" should exist
    And that file should be valid JSON
    And that file should contain "two"
    And that file should contain "three"

  Scenario: Select to YAML
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:two, :three) |> write("$TEMPDIR/select.yaml")
      ```
    Then the file "$TEMPDIR/select.yaml" should exist
    And that file should be valid YAML
    And that file should contain "two:"
    And that file should contain "three:"

  Scenario: Select to Avro
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:one, :two) |> write("$TEMPDIR/select.avro")
      ```
    Then the file "$TEMPDIR/select.avro" should exist
    And that file should be valid Avro

  Scenario: Select to Parquet
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> select(:id, :first_name, :email) |> write("$TEMPDIR/select.parquet")
      ```
    Then the file "$TEMPDIR/select.parquet" should exist
    And that file should be valid Parquet
