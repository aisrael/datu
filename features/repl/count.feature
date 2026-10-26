Feature: Count

  Scenario: Row count from Parquet
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> count()
      ```
    Then the command should succeed
    And the output should contain "Pipeline: read("fixtures/table.parquet") |> count()"
    And the output should contain "3"

  Scenario: Row count after filter
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.parquet") |> filter("gender = 'Female'") |> count()
      ```
    Then the command should succeed
    And the output should contain "482"

  Scenario: Row count after select
    When the REPL is ran and the user types:
      ```
      read("fixtures/table.parquet") |> select(:one) |> count()
      ```
    Then the command should succeed
    And the output should contain "Pipeline: read("fixtures/table.parquet") |> select(:one) |> count()"
    And the output should contain "3"

  Scenario: Row count from ORC
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.orc") |> count()
      ```
    Then the command should succeed
    And the output should contain "1000"
