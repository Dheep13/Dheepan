import { cucumberPreprocessor } from "./plugins/cucumberPreprocessor";

const xwebCypressConfig = require("@xweb/cypress-utils/lib/plugins/config");
const xwebCypressLog = require("@xweb/cypress-utils/lib/plugins/log");

export function cypressE2ESetup() {
  return {
    specPattern: "**/*.feature",
    // plugins
    async setupNodeEvents(
      on: Cypress.PluginEvents,
      config: Cypress.PluginConfigOptions
    ): Promise<Cypress.PluginConfigOptions> {
      xwebCypressLog.installLogsPrinter(on, config);
      xwebCypressConfig.setCommonConfig(on, config);
      await cucumberPreprocessor(on, config);
      return config;
    },
  };
}
