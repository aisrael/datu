Feature: Conversion

  Scenario:
    When the REPL is ran and the user types:
      ```
      read("fixtures/userdata.parquet") |> write("$TEMPDIR/userdata.csv")
      ```
    Then the file "$TEMPDIR/userdata.csv" should exist
    And that file should be a CSV file
    And the first line of that file should be: "registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments"
