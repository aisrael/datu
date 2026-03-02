Feature: Conversion

  Scenario: Parquet to CSV
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.parquet") |> write("$TEMPDIR/userdata.csv")
      ```
    Then the file "$TEMPDIR/userdata.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments"

  Scenario: Parquet to JSON
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> write("$TEMPDIR/table.json")
      ```
    Then the file "$TEMPDIR/table.json" should exist
    And that file should be valid JSON
    And that file should contain "two"
    And that file should contain "foo"

  Scenario: Parquet to YAML
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> write("$TEMPDIR/table.yaml")
      ```
    Then the file "$TEMPDIR/table.yaml" should exist
    And that file should be valid YAML
    And that file should contain "two:"
    And that file should contain "foo"

  Scenario: Parquet to Avro
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> write("$TEMPDIR/table.avro")
      ```
    Then the file "$TEMPDIR/table.avro" should exist
    And that file should be valid Avro

  Scenario: Parquet to XLSX
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> write("$TEMPDIR/table.xlsx")
      ```
    Then the file "$TEMPDIR/table.xlsx" should exist
    And that file should be valid XLSX

  Scenario: Avro to CSV
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> write("$TEMPDIR/userdata5.csv")
      ```
    Then the file "$TEMPDIR/userdata5.csv" should exist
    And that file should be a CSV file
    And the first line of that file should contain "id"
    And the first line of that file should contain "first_name"

  Scenario: Avro to JSON
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> write("$TEMPDIR/userdata5.json")
      ```
    Then the file "$TEMPDIR/userdata5.json" should exist
    And that file should be valid JSON

  Scenario: Avro to YAML
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> write("$TEMPDIR/userdata5.yaml")
      ```
    Then the file "$TEMPDIR/userdata5.yaml" should exist
    And that file should be valid YAML
    And that file should contain "id:"
    And that file should contain "first_name:"

  Scenario: Avro to Parquet
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> write("$TEMPDIR/userdata5.parquet")
      ```
    Then the file "$TEMPDIR/userdata5.parquet" should exist
    And that file should be valid Parquet

  Scenario: Avro to ORC
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> write("$TEMPDIR/userdata5.orc")
      ```
    Then the file "$TEMPDIR/userdata5.orc" should exist
    And that file should be valid ORC

  Scenario: Avro to XLSX
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> write("$TEMPDIR/userdata5.xlsx")
      ```
    Then the file "$TEMPDIR/userdata5.xlsx" should exist
    And that file should be valid XLSX

  Scenario: ORC to CSV
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.orc") |> write("$TEMPDIR/userdata_orc.csv")
      ```
    Then the file "$TEMPDIR/userdata_orc.csv" should exist
    And that file should be a CSV file

  Scenario: ORC to JSON
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.orc") |> write("$TEMPDIR/userdata_orc.json")
      ```
    Then the file "$TEMPDIR/userdata_orc.json" should exist
    And that file should be valid JSON

  Scenario: ORC to YAML
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.orc") |> write("$TEMPDIR/userdata_orc.yaml")
      ```
    Then the file "$TEMPDIR/userdata_orc.yaml" should exist
    And that file should be valid YAML

  Scenario: ORC to Parquet
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.orc") |> write("$TEMPDIR/userdata_orc.parquet")
      ```
    Then the file "$TEMPDIR/userdata_orc.parquet" should exist
    And that file should be valid Parquet

  Scenario: ORC to Avro
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.orc") |> write("$TEMPDIR/userdata_orc.avro")
      ```
    Then the file "$TEMPDIR/userdata_orc.avro" should exist
    And that file should be valid Avro

  Scenario: ORC to XLSX
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.orc") |> write("$TEMPDIR/userdata_orc.xlsx")
      ```
    Then the file "$TEMPDIR/userdata_orc.xlsx" should exist
    And that file should be valid XLSX

  Scenario: Parquet to CSV with select
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:two, :four) |> write("$TEMPDIR/table_select.csv")
      ```
    Then the file "$TEMPDIR/table_select.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "two,four"
    And that file should have 4 lines

  Scenario: Parquet to CSV with head
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> head(2) |> write("$TEMPDIR/table_head.csv")
      ```
    Then the file "$TEMPDIR/table_head.csv" should exist
    And that file should be a CSV file
    And that file should have 3 lines

  Scenario: Avro to JSON with select and head
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> select(:id, :first_name, :email) |> head(5) |> write("$TEMPDIR/userdata5_subset.json")
      ```
    Then the file "$TEMPDIR/userdata5_subset.json" should exist
    And that file should be valid JSON
    And that file should contain "id"
    And that file should contain "first_name"
    And that file should contain "email"

  Scenario: Parquet to YAML with select
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:two, :four) |> write("$TEMPDIR/table_select.yaml")
      ```
    Then the file "$TEMPDIR/table_select.yaml" should exist
    And that file should be valid YAML
    And that file should contain "two:"
    And that file should contain "four:"

  Scenario: Avro to CSV with head
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata5.avro") |> head(10) |> write("$TEMPDIR/userdata5_head.csv")
      ```
    Then the file "$TEMPDIR/userdata5_head.csv" should exist
    And that file should be a CSV file
    And the first line of that file should contain "id"
    And that file should have 11 lines
