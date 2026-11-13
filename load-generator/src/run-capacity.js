import {config} from "./config.js";
import {runCapacity} from "./capacity.js";

try {
  const report = await runCapacity(config());
  console.log(`Report: ${report.artifactDirectory}/summary.md`);
  process.exitCode = report.result === "PASS" ? 0 : 1;
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
