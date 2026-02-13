Feature: Convert
  Convert between Parquet, Avro, ORC, CSV, JSON, YAML, and XLSX file formats.

  Scenario: Parquet to Avro
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table.avro`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table.avro"
    And the file "$TEMPDIR/table.avro" should exist

  Scenario: Avro to Parquet
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.parquet`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5.parquet"
    And the file "$TEMPDIR/userdata5.parquet" should exist

  Scenario: Avro to ORC
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5.orc"
    And the file "$TEMPDIR/userdata5.orc" should exist

  Scenario: ORC to Parquet
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu convert $TEMPDIR/userdata5.orc $TEMPDIR/userdata5.parquet`
    Then the command should succeed
    And the output should contain "Converting $TEMPDIR/userdata5.orc to $TEMPDIR/userdata5.parquet"
    And the file "$TEMPDIR/userdata5.parquet" should exist

  Scenario: Parquet to ORC
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/table.orc --select id,first_name --limit 10`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/table.orc"
    And the file "$TEMPDIR/table.orc" should exist

  Scenario: Parquet to CSV
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table.csv`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table.csv"
    And the file "$TEMPDIR/table.csv" should exist
    And the first line of that file should contain "one,two"
    And that file should have 4 lines

  Scenario: Avro to CSV
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.csv`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5.csv"
    And the file "$TEMPDIR/userdata5.csv" should exist
    And the first line of that file should contain "id,first_name"
    And that file should have 1001 lines

  Scenario: ORC to CSV
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu convert $TEMPDIR/userdata5.orc $TEMPDIR/userdata5.csv`
    Then the command should succeed
    And the output should contain "Converting $TEMPDIR/userdata5.orc to $TEMPDIR/userdata5.csv"
    And the file "$TEMPDIR/userdata5.csv" should exist
    And the first line of that file should contain "id,first_name"

  Scenario: Parquet to CSV with --select
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table_select.csv --select two,four`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_select.csv"
    And the file "$TEMPDIR/table_select.csv" should exist
    And the first line of that file should contain "two,four"
    And that file should have 4 lines

  Scenario: Avro to CSV with --select
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5_select.csv --select id,first_name,email`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5_select.csv"
    And the file "$TEMPDIR/userdata5_select.csv" should exist
    And the first line of that file should contain "id,first_name,email"
    And that file should have 1001 lines

  Scenario: Parquet to Avro with --limit
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table_limit.avro --limit 2`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_limit.avro"
    And the file "$TEMPDIR/table_limit.avro" should exist

  Scenario: Avro to CSV with --limit and --select
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5_limit_select.csv --limit 3 --select id,email`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5_limit_select.csv"
    And the file "$TEMPDIR/userdata5_limit_select.csv" should exist
    And the first line of that file should contain "id,email"
    And that file should have 4 lines

  Scenario: Parquet to JSON
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table.json`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table.json"
    And the file "$TEMPDIR/table.json" should exist
    And the file "$TEMPDIR/table.json" should contain:
      ```
      [{"one":-1.0,"two":"foo","three":true,"four":"2022-12-23T00:00:00Z","five":"2022-12-23T11:43:49","__index_level_0__":"a"},{"two":"bar","three":false,"four":"2021-12-23T00:00:00Z","five":"2021-12-23T12:44:50","__index_level_0__":"b"},{"one":2.5,"two":"baz","four":"2020-12-23T00:00:00Z","five":"2020-12-23T13:45:51","__index_level_0__":"c"}]
      ```

  Scenario: Parquet to JSON (with `--json-pretty`)
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table_sparse.json --json-pretty`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_sparse.json"
    And the file "$TEMPDIR/table_sparse.json" should exist
    And the file "$TEMPDIR/table_sparse.json" should contain:
      ```
      [
        {
          "__index_level_0__": "a",
          "five": "2022-12-23T11:43:49",
          "four": "2022-12-23T00:00:00Z",
          "one": -1.0,
          "three": true,
          "two": "foo"
        },
        {
          "__index_level_0__": "b",
          "five": "2021-12-23T12:44:50",
          "four": "2021-12-23T00:00:00Z",
          "three": false,
          "two": "bar"
        },
        {
          "__index_level_0__": "c",
          "five": "2020-12-23T13:45:51",
          "four": "2020-12-23T00:00:00Z",
          "one": 2.5,
          "two": "baz"
        }
      ]
      ```

  Scenario: Avro to JSON
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.json --json-pretty`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5.json"
    And the file "$TEMPDIR/userdata5.json" should exist

  Scenario: Parquet to YAML (default sparse)
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table.yaml`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table.yaml"
    And the file "$TEMPDIR/table.yaml" should exist
    And the file "$TEMPDIR/table.yaml" should contain:
      ```
      - one: -1
        two: foo
        three: true
        four: "2022-12-23T00:00:00Z"
        five: "2022-12-23T11:43:49"
        __index_level_0__: a
      - two: bar
        three: false
        four: "2021-12-23T00:00:00Z"
        five: "2021-12-23T12:44:50"
        __index_level_0__: b
      - one: 2.5
        two: baz
        four: "2020-12-23T00:00:00Z"
        five: "2020-12-23T13:45:51"
        __index_level_0__: c
      ```

  Scenario: Parquet to JSON with sparse=false
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table_no_sparse.json --sparse=false`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_no_sparse.json"
    And the file "$TEMPDIR/table_no_sparse.json" should exist
    And that file should contain "one"
    And that file should contain "null"

  Scenario: Parquet to YAML with sparse=false
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table_no_sparse.yaml --sparse=false`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_no_sparse.yaml"
    And the file "$TEMPDIR/table_no_sparse.yaml" should exist
    And that file should contain "one:"

  Scenario: Avro to YAML
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.yaml`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5.yaml"
    And the file "$TEMPDIR/userdata5.yaml" should exist
    And that file should contain "id:"
    And that file should contain "first_name:"

  Scenario: ORC to JSON
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu convert $TEMPDIR/userdata5.orc $TEMPDIR/userdata5.json --json-pretty`
    Then the command should succeed
    And the output should contain "Converting $TEMPDIR/userdata5.orc to $TEMPDIR/userdata5.json"
    And the file "$TEMPDIR/userdata5.json" should exist

  Scenario: ORC to YAML
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu convert $TEMPDIR/userdata5.orc $TEMPDIR/userdata5.yaml`
    Then the command should succeed
    And the output should contain "Converting $TEMPDIR/userdata5.orc to $TEMPDIR/userdata5.yaml"
    And the file "$TEMPDIR/userdata5.yaml" should exist
    And that file should contain "id:"
    And that file should contain "first_name:"

  Scenario: Parquet to YAML with --select
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table_select.yaml --select two,four`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_select.yaml"
    And the file "$TEMPDIR/table_select.yaml" should exist
    And that file should contain "two:"
    And that file should contain "four:"

  Scenario: Parquet to YAML with --limit
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table_limit.yaml --limit 2`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_limit.yaml"
    And the file "$TEMPDIR/table_limit.yaml" should exist

  Scenario: Avro to YAML with .yml extension
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.yml`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5.yml"
    And the file "$TEMPDIR/userdata5.yml" should exist
    And that file should contain "email:"

  Scenario: Parquet to XLSX
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table.xlsx`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table.xlsx"
    And the file "$TEMPDIR/table.xlsx" should exist

  Scenario: Avro to XLSX
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.xlsx`
    Then the command should succeed
    And the output should contain "Converting fixtures/userdata5.avro to $TEMPDIR/userdata5.xlsx"
    And the file "$TEMPDIR/userdata5.xlsx" should exist

  Scenario: ORC to CSV with --select
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name,email --limit 10`
    Then the command should succeed
    When I run `datu convert $TEMPDIR/userdata5.orc $TEMPDIR/userdata5_select.csv --select id,first_name,email`
    Then the command should succeed
    And the output should contain "Converting $TEMPDIR/userdata5.orc to $TEMPDIR/userdata5_select.csv"
    And the file "$TEMPDIR/userdata5_select.csv" should exist
    And the first line of that file should contain "id,first_name,email"

  Scenario: ORC to XLSX
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu convert $TEMPDIR/userdata5.orc $TEMPDIR/userdata5.xlsx`
    Then the command should succeed
    And the output should contain "Converting $TEMPDIR/userdata5.orc to $TEMPDIR/userdata5.xlsx"
    And the file "$TEMPDIR/userdata5.xlsx" should exist

  Scenario: ORC to Parquet with --limit
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu convert $TEMPDIR/userdata5.orc $TEMPDIR/userdata5_limit.parquet --limit 5`
    Then the command should succeed
    And the output should contain "Converting $TEMPDIR/userdata5.orc to $TEMPDIR/userdata5_limit.parquet"
    And the file "$TEMPDIR/userdata5_limit.parquet" should exist

  Scenario: Parquet to XLSX with --select
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table_select.xlsx --select two,four`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_select.xlsx"
    And the file "$TEMPDIR/table_select.xlsx" should exist

  Scenario: Parquet to XLSX with --limit
    When I run `datu convert fixtures/table.parquet $TEMPDIR/table_limit.xlsx --limit 2`
    Then the command should succeed
    And the output should contain "Converting fixtures/table.parquet to $TEMPDIR/table_limit.xlsx"
    And the file "$TEMPDIR/table_limit.xlsx" should exist
