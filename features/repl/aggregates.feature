Feature: Aggregate Functions

  Scenario: Sum
    Given a Parquet file with the following data:
      ```
      item_id,quantity
      1,11
      2,22
      3,33
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> select(sum(:quantity))
      ```
    Then the command should succeed
    And the output should contain "66"

  Scenario: Group by with sum
    Given a Parquet file with the following data:
      ```
      item_id,quantity
      1,10
      1,20
      2,5
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> group_by(:item_id) |> select(:item_id, sum(:quantity))
      ```
    Then the command should succeed
    And the output should contain "30"
    And the output should contain "5"

  Scenario: Average
    Given a Parquet file with the following data:
      ```
      item_id,quantity
      1,11
      2,22
      3,33
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> select(avg(:quantity))
      ```
    Then the command should succeed
    And the output should contain "22"

  Scenario: Group by with average
    Given a Parquet file with the following data:
      ```
      item_id,quantity
      1,10
      1,20
      2,5
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> group_by(:item_id) |> select(:item_id, avg(:quantity))
      ```
    Then the command should succeed
    And the output should contain "15"
    And the output should contain "5"
