Feature: Count

  Scenario: Row count from Parquet
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> count()
      ```
    Then the command should succeed
