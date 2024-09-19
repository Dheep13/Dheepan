Cypress

### How to run all the tests

`cd cypress && npm i && npm run test`

### How to test regarding to each feature file

`cd cypress && npm i && npm run start`

and select the file you want to run

### An example of feature file containing for UI elements interaction in current DAS web client

```
Feature: Create Position

Scenario: Test create position
  Given I logged in
  Given I open chat window and reset conversation "true"

  # verify policy
  When I send messgae "I want to hire a consultant"
  Then I should find "Hiring Policy Process" in "a"
  Then I should find "Create Position Process" in "a"

  # select direct report
  When I click "ui5-button" "Yes, I do"
  When I click "ui5-button" "Yes"
  When I click "ui5-button" "Select"

  # update position details
  Then I should find "Key Position Details" in "ui-integration-card"
  When I click "ui5-button" "Yes"
  When I click "ui5-button" "Location"
  When I click "ui5-button" "Boston"
  Given I wait "3s"
  When I send messgae "done"

  # pick a date
  When I click "ui5-button" "No, I'll pick a date"
  When I click "Date Picker Button"
  When I click "Today On Calendar"
  When I click "ui5-button" "Submit"

  # verify position details and send
  Then I should find "Boston" in "ui-integration-card"
  When I click "ui5-button" "Yes, seek approval"
  Then I should find "View Position Request" in "ui5-illustrated-message"
```

### Available feature scripts

```
sfsf_das_bot/cypress/node_modules/@xweb/cypress-common/dist/step_definitions/commonImpl.js
sfsf_das_bot/cypress/cypress/support/step_definitions/commonSteps.ts
```
