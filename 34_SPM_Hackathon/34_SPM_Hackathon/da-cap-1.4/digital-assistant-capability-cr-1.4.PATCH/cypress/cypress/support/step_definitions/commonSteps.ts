/* eslint-disable no-empty-function */

import { Then, When, Given } from "@badeball/cypress-cucumber-preprocessor";
import {
  getIdentity,
  getCompany,
} from "@xweb/cypress-common/dist/providers/identities";
import {
  getChatWindowElement,
  getChatWindowElementWithPermissionFallback,
  getChatWindowMainDiv,
  openChatWindow,
} from "../providers/daElementProvider";
import "../providers";
import { DUMMY_TEXT, FALLBACK_MSG } from "../constants";

Given(`I logged in`, () => {
  const defaultUser = Cypress.env("defaultUser");
  cy.session(defaultUser, () => {
    // TODO: upload pass to valut
    const { username, password = "China001" } = getIdentity(defaultUser);
    const company = getCompany();

    // workaround for blank page
    cy.visit(
      "https://qacand.hcm.ondemand.com/saml2/Login?company=STEDASIT02&RelayState=%2Fsf%2Fhome%3Fbplte_company%3DSTEDASIT02&_s.crb=3rMP4iEdhK3i1k5iroIpvvqXG5mroQ3e%252ffUI6TIa0f4%253d"
    );
    // cy.get("#__input0-inner").type(company);
    // cy.get("#__button0").click();
    cy.get("#j_username").type(username);
    cy.get("#j_password").type(password as string);
    cy.get("#logOnFormSubmit").click().wait(15000);

    // first time opening chat window which takes longer time to keep required info in session to improve
    // test run time
    openChatWindow().wait(10000);

    // TODO: remove
    // workaround for relogin
    getChatWindowElement().find("#j_username-group").type(username);
    getChatWindowElement()
      .find("#j_password")
      .type(password as string);
    getChatWindowElement().find("#logOnFormSubmit").click().wait(15000);
  });
});

Given(
  `I open chat window and reset conversation {string}`,
  (resetConversation) => {
    cy.visit("/");
    // not first time opening chat window which takes less time
    openChatWindow();
    // reset conversation if possible
    resetConversation === "true" &&
      getChatWindowMainDiv().then(() =>
        getChatWindowElement()
          .find("ui5-button[icon^=restart]")
          .click()
          .wait(1000)
      );
  }
);

Given(`I reset conversation`, () =>
  getChatWindowElement().find("ui5-button[icon^=restart]").click()
);

Given(`I logged in as {string} and open chat window`, (user: string) => {
  const { username, password = "China001" } = getIdentity(user);
  const company = getCompany();

  cy.visit("/");
  cy.get("#__input0-inner").type(company);
  cy.get("#__button0").click();
  cy.get("#j_username").type(username);
  cy.get("#j_password").type(password as string);
  cy.get("#logOnFormSubmit").click().wait(15000);

  cy.window()
    .then((win: any) => {
      win.sap.das.webclient.toggle();
    })
    .wait(10000);
});

When(`I send messgae {string}`, (msg: string) => {
  cy.window()
    .then((win: any) => {
      win.sap.das.webclient.sendMessage(msg);
    })
    .wait(2000);
});

When(`I send dummy messgae with length {string}`, (msg: string) => {
  cy.window()
    .then((win: any) => {
      win.sap.das.webclient.sendMessage(DUMMY_TEXT.substring(0, parseInt(msg)));
    })
    .wait(2000);
});

When(`I click {string} {string}`, (type: string, name: string) => {
  getChatWindowElementWithPermissionFallback(() =>
    getChatWindowElement()
      .find(type)
      .contains(name, { matchCase: false })
      .last()
      .click({ force: true })
      .wait(2000)
  );
});

Then("I should find {string} in {string}", (content: string, type: string) => {
  getChatWindowElementWithPermissionFallback(() =>
    getChatWindowElement()
      .find(type)
      .contains(content, { matchCase: false })
      .should("have.length.at.least", 1)
      .wait(500)
  );
});

Then("I should find fallback msg {string}", (type: string) => {
  getChatWindowElementWithPermissionFallback(() =>
    getChatWindowElement()
      .find("span")
      .contains((FALLBACK_MSG as Record<string, string>)[type], {
        matchCase: false,
      })
      .should("have.length.at.least", 1)
  );
});

Cypress.on("uncaught:exception", (err, runnable) => {
  // returning false here prevents Cypress from
  // failing the test
  return false;
});
