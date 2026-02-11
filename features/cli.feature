Feature: CLI

  Scenario: Print version
    When I run `datu --version`
    Then the output should contain "datu 0.2.1"

  Scenario: Print help with help subcommand
    When I run `datu help`
    Then the output should be:
      ```
      datu - a data file utility
      
      Usage: datu <COMMAND>
      
      Commands:
        convert  convert between file formats
        head     print the first n lines of a file
        tail     print the last n lines of a file
        schema   display the schema of a file
        version  print the datu version
        help     Print this message or the help of the given subcommand(s)
      
      Options:
        -h, --help     Print help
        -V, --version  Print version
      ```

  Scenario: Print help with -h
    When I run `datu -h`
    Then the output should be:
      ```
      datu - a data file utility
      
      Usage: datu <COMMAND>
      
      Commands:
        convert  convert between file formats
        head     print the first n lines of a file
        tail     print the last n lines of a file
        schema   display the schema of a file
        version  print the datu version
        help     Print this message or the help of the given subcommand(s)
      
      Options:
        -h, --help     Print help
        -V, --version  Print version
      ```
