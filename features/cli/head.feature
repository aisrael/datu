Feature: Head
  Print the first N rows of a Parquet, Avro, or ORC file as CSV.

  Scenario: Head Parquet default (10 lines)
    When I run `datu head fixtures/userdata.parquet`
    Then the command should succeed
    And the output should have a header and 10 lines

  Scenario: Head Parquet with -n 2
    When I run `datu head fixtures/userdata.parquet -n 2`
    Then the command should succeed
    And the output should have a header and 2 lines

  Scenario: Head Avro default (10 lines)
    When I run `datu head fixtures/userdata5.avro`
    Then the command should succeed
    And the output should have a header and 10 lines
    And the first line should be: registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments

  Scenario: Head Avro with -n 3
    When I run `datu head fixtures/userdata5.avro -n 3`
    Then the command should succeed
    And the output should have a header and 3 lines
    And the first line should be: registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments

  Scenario: Head Parquet with --select
    When I run `datu head fixtures/userdata.parquet -n 2 --select id,last_name`
    Then the command should succeed
    And the output should have a header and 2 lines
    And the first line should be: id,last_name

  Scenario: Head Avro with --select
    When I run `datu head fixtures/userdata5.avro -n 2 --select id,email`
    Then the command should succeed
    And the output should have a header and 2 lines
    And the first line should be: id,email

  Scenario: Head ORC default (10 lines)
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu head $TEMPDIR/userdata5.orc`
    Then the command should succeed
    And the output should have a header and 10 lines
    And the first line should be: id,first_name

  Scenario: Head ORC with -n 3
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu head $TEMPDIR/userdata5.orc -n 3`
    Then the command should succeed
    And the output should have a header and 3 lines
    And the first line should be: id,first_name

  Scenario: Head ORC with --select
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu head $TEMPDIR/userdata5.orc -n 2 --select id,first_name`
    Then the command should succeed
    And the output should have a header and 2 lines
    And the first line should be: id,first_name

  Scenario: Head ORC with --output csv
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu head $TEMPDIR/userdata5.orc -n 2 --output csv`
    Then the command should succeed
    And the output should have a header and 2 lines
    And the first line should be: id,first_name

  Scenario: Head ORC with --output json
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu head $TEMPDIR/userdata5.orc -n 2 --output json`
    Then the command should succeed
    And the output should be valid JSON
    And the output should contain "["
    And the output should contain "id"

  Scenario: Head ORC with --output yaml
    When I run `datu convert fixtures/userdata5.avro $TEMPDIR/userdata5.orc --select id,first_name --limit 10`
    Then the command should succeed
    When I run `datu head $TEMPDIR/userdata5.orc -n 2 --output yaml`
    Then the command should succeed
    And the output should be valid YAML
    And the output should contain "first_name"
    And the output should contain "id"

  Scenario: Head Parquet with --output csv
    When I run `datu head fixtures/userdata.parquet -n 2 --output csv`
    Then the command should succeed
    And the output should have a header and 2 lines

  Scenario: Head Parquet with --output json
    When I run `datu head fixtures/userdata.parquet -n 2 --output json`
    Then the command should succeed
    And the output should be valid JSON
    And the output should contain "["
    And the output should contain "id"

  Scenario: Head Avro with --output csv
    When I run `datu head fixtures/userdata5.avro -n 2 --output csv`
    Then the command should succeed
    And the output should have a header and 2 lines
    And the first line should be: registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments

  Scenario: Head Avro with --output json
    When I run `datu head fixtures/userdata5.avro -n 2 --output json`
    Then the command should succeed
    And the output should be valid JSON
    And the output should contain "["
    And the output should contain "first_name"

  Scenario: Head Parquet with --output yaml
    When I run `datu head fixtures/userdata.parquet -n 2 --output yaml`
    Then the command should succeed
    And the output should be valid YAML
    And the output should contain "id"
    And the output should contain "first_name"

  Scenario: Head Avro with --output yaml
    When I run `datu head fixtures/userdata5.avro -n 2 --output yaml`
    Then the command should succeed
    And the output should be valid YAML
    And the output should contain "first_name"
    And the output should contain "email"
