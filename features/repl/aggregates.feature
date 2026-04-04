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
    And the output should contain "1,15.0"
    And the output should contain "2,5.0"

  Scenario: Minimum
    Given a Parquet file with the following data:
      ```
      item_id,quantity
      1,11
      2,22
      3,33
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> select(min(:quantity))
      ```
    Then the command should succeed
    And the output should contain "11"

  Scenario: Maximum
    Given a Parquet file with the following data:
      ```
      item_id,quantity
      1,11
      2,22
      3,33
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> select(max(:quantity))
      ```
    Then the command should succeed
    And the output should contain "33"

  Scenario: Group by with minimum
    Given a Parquet file with the following data:
      ```
      item_id,quantity
      1,10
      1,20
      2,5
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> group_by(:item_id) |> select(:item_id, min(:quantity))
      ```
    Then the command should succeed
    And the output should contain "1,10"
    And the output should contain "2,5"

  Scenario: Group by with maximum
    Given a Parquet file with the following data:
      ```
      item_id,quantity
      1,10
      1,20
      2,5
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> group_by(:item_id) |> select(:item_id, max(:quantity))
      ```
    Then the command should succeed
    And the output should contain "1,20"
    And the output should contain "2,5"

  Scenario: Count non-null values
    Given a Parquet file with the following data:
      ```
      item_id,quantity
      1,11
      2,22
      3,33
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> select(count(:quantity))
      ```
    Then the command should succeed
    And the output should contain "3"

  Scenario: Count distinct values
    Given a Parquet file with the following data:
      ```
      item_id,region
      1,US
      2,US
      3,EU
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> select(count_distinct(:region))
      ```
    Then the command should succeed
    And the output should contain "2"

  Scenario: Group by with count
    Given a Parquet file with the following data:
      ```
      item_id,quantity
      1,10
      1,20
      2,5
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> group_by(:item_id) |> select(:item_id, count(:quantity))
      ```
    Then the command should succeed
    And the output should contain "1,2"
    And the output should contain "2,1"

  Scenario: Group by with count distinct
    Given a Parquet file with the following data:
      ```
      item_id,region
      1,US
      1,US
      2,EU
      ```
    When the REPL is ran and the user types:
      ```
      read("$TEMPDIR/input.parquet") |> group_by(:item_id) |> select(:item_id, count_distinct(:region))
      ```
    Then the command should succeed
    And the output should contain "1,1"
    And the output should contain "2,1"
