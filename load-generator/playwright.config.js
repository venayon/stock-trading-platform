import {defineConfig} from "@playwright/test";

const suite = process.argv.includes('--project=perf') ? 'perf' : 'api';
export default defineConfig({
  outputDir: `test-results/${suite}`,
  testDir: "./tests",
  fullyParallel: true,
  retries: 0,
  workers: 2,
  timeout: 45000,
  expect: {timeout: 30000},
  reporter: [
    ["list"],
    ["html", {outputFolder: `reports/${suite}-html`, open: "never"}],
    ["junit", {outputFile: `reports/${suite}-junit.xml`}],
  ],
  use: {
    baseURL: process.env.TRADER_URL ?? "http://localhost:8081",
    timeout: 10000,
  },
  projects: [
    {name: "api", testMatch: "api/**/*.spec.js"},
    {name: "perf", testMatch: "perf/**/*.spec.js"},
  ],
});
