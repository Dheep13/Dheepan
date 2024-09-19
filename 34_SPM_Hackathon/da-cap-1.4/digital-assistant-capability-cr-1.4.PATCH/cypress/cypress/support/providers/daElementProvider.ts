import { FALLBACK_MSG } from "../constants";

const getIframeDocument = () =>
  cy.get("#cai-webclient-iframe").its("0.contentDocument").should("exist");

const getIframeBody = () =>
  getIframeDocument()
    // automatically retries until body is loaded
    .its("body")
    .should("not.be.undefined")
    // wraps "body" DOM element to allow
    // chaining more Cypress commands, like ".find(...)"
    .then(cy.wrap);

export const getChatWindowElement = () => getIframeBody();

export const getChatWindowMainDiv = () =>
  cy
    .get("#cai-webclient-iframe")
    .its("0.contentDocument.body")
    .find("#das-webclient-div");

export const getChatWindowElementWithPermissionFallback = (cb: Function) => {
  getChatWindowElement()
    .find("span[class^='dasText']")
    .last()
    .then(($span) => {
      const innerText = $span.text().toLowerCase();
      if (innerText.includes(FALLBACK_MSG.PERMISSION.toLowerCase())) {
        cy.log("No Permission:" + $span.text());
      } else if (
        innerText.includes(FALLBACK_MSG.NO_RECORD.toLocaleLowerCase())
      ) {
        cy.log("No Record:" + $span.text());
      } else {
        cb();
      }
    });
};

export const getElementInChatWindow = (selector: string) =>
  getIframeBody().find(selector);

export const openChatWindow = () => {
  let intervalId: any;
  return cy
    .window()
    .then((win: any) =>
      // wait util das is setup to open chat window
      new Promise((resolve, _) => {
        intervalId = setInterval(() => {
          if (win.sap) {
            win.sap.das.webclient.toggle();
            resolve(true);
          }
        }, 200);
      }).then(() => clearInterval(intervalId))
    )
    .wait(8000);
};
