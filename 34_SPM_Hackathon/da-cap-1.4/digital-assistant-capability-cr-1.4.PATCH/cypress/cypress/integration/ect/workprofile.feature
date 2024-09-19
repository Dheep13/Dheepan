Feature: Work profile

  Scenario Outline: Test view <utterance>
    Given I logged in
    Given I open chat window and reset conversation "true"
    When I send messgae "view my <utterance>"
    Then I should find "<verify_msg>" in "<elem_type>"

    Given I reset conversation
    When I send messgae "view <utterance>"
    Then I should find "Whose information do you want to view?" in "span"
    When I send messgae "alex"
    When I click "ui5-button" "Select"
    Then I should find "<verify_msg>" in "<elem_type>"

  Examples: 
    | utterance  | verify_msg                             | elem_type |
    | email      | Here is the information you requested: | span      |
    | birthday   | Here is the information you requested: | span      |
    | location   | Here is the information you requested: | span      |
    | title      | Here is the information you requested: | span      |