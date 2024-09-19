import { defineConfig } from "cypress";
import { cypressE2ESetup } from "./cypress/cypressE2ESetup";

export default defineConfig({
  chromeWebSecurity: false,
  defaultCommandTimeout: 20000,
  e2e: cypressE2ESetup(),
});
