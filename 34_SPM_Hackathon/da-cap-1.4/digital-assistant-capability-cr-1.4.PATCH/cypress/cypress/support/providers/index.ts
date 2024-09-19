import { addSelectors } from "@xweb/cypress-common";
import {
  getElementInChatWindow,
  getChatWindowElement,
} from "./daElementProvider";

const selectors = {
  "Restart Button": "ui5-button[icon^=restart]",
} as any;

addSelectors(
  (function () {
    const newSelectors: any = {};
    for (const key in selectors) {
      newSelectors[key] = () => getElementInChatWindow(selectors[key]);
    }

    newSelectors["Today On Calendar"] = () =>
      getChatWindowElement()
        .find("ui5-static-area-item")
        .last()
        .shadow()
        .find("ui5-calendar")
        .shadow()
        .find("ui5-daypicker")
        .shadow()
        .find("div[aria-label^=Today]");

    newSelectors["Date Picker Button"] = () =>
      getChatWindowElement().find("ui5-date-picker").shadow().find("ui5-icon");

    return newSelectors;
  })()
);
// addFeatures({});
// addMessages({});
// addPermissions({});
// addUrls({});
// addProxyUrls({});
