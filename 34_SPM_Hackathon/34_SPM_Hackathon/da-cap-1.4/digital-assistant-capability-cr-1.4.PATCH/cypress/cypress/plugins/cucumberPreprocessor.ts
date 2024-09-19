import webpackPreprocessor from "@cypress/webpack-preprocessor";
import { addCucumberPreprocessorPlugin } from "@badeball/cypress-cucumber-preprocessor";

export async function cucumberPreprocessor(on: Cypress.PluginEvents, config: Cypress.PluginConfigOptions) {
  // This is required for the preprocessor to be able to generate JSON reports after each run, and more,
  await addCucumberPreprocessorPlugin(on, config);
  on(
    "file:preprocessor",
    webpackPreprocessor({
      webpackOptions: {
        // node: { fs: "empty", child_process: "empty", readline: "empty" },
        resolve: {
          extensions: [".ts", ".js"],
          fallback: {
            path: false
          }
        },
        module: {
          rules: [
            {
              test: /\.ts$/,
              exclude: [/node_modules/],
              use: [
                {
                  loader: "ts-loader",
                  options: {
                    compilerOptions: { noEmit: false }
                  }
                }
              ]
            },
            {
              test: /\.feature$/,
              use: [
                {
                  loader: "@badeball/cypress-cucumber-preprocessor/webpack",
                  options: config
                }
              ]
            }
          ]
        }
      }
    })
  );
}
