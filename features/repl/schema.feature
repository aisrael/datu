Feature: Schema

  Scenario: Schema from Parquet
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> schema()
      ```
    Then the command should succeed

  Scenario: Schema with select
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:one, :two) |> schema()
      ```
    Then the command should succeed
