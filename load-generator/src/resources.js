import {execFile} from "node:child_process";
import {promisify} from "node:util";
import {request} from "@playwright/test";
import {call, sleep} from "./client.js";
import {fromMetrics} from "./stats.js";

const exec = promisify(execFile);

function bytes(text) {
  const m = text.trim().match(/^([\d.]+)\s*([kMGT]?i?B)$/i);
  if (!m) return null;
  const unit = m[2].toLowerCase(),
    prefix = unit[0],
    power = {k: 1, m: 2, g: 3, t: 4}[prefix] ?? 0;
  return Number(m[1]) * (unit.includes("i") ? 1024 : 1000) ** power;
}

export async function resources(config, write) {
  const context = await request.newContext({timeout: 5000}),
    warnings = new Set(),
    samples = [],
    docker = [];
  let running = true,
    inProgress;
  const endpoints = {
      trader: config.traderUrl,
      processor: config.processorUrl,
    },
    pids = {};
  for (const [name, url] of Object.entries(endpoints)) {
    const supplied = config[`${name}Pid`];
    if (supplied) {
      if (!/^\d+$/.test(supplied)) throw new Error(`Invalid ${name} PID`);
      pids[name] = supplied;
    } else if (
      ["localhost", "127.0.0.1", "[::1]"].includes(new URL(url).hostname)
    ) {
      try {
        const result = await exec(
          "lsof",
          ["-t", `-iTCP:${new URL(url).port || 80}`, "-sTCP:LISTEN"],
          {timeout: 2000},
        );
        const ids = [...new Set(result.stdout.trim().split(/\s+/))];
        if (ids.length === 1) pids[name] = ids[0];
        else warnings.add(`${name}: ambiguous port ownership; RSS unavailable`);
      } catch {
        warnings.add(
          `${name}: PID discovery unavailable; set ${name.toUpperCase()}_PID for RSS`,
        );
      }
    }
    if (pids[name]) {
      try {
        const owner = await exec("ps", ["-p", pids[name], "-o", "comm="], {timeout: 2000});
        if (!/java$/i.test(owner.stdout.trim())) throw new Error("Not a JVM");
      } catch {
        delete pids[name];
        warnings.add(`${name}: port/PID owner is not a verified host JVM; RSS unavailable`);
      }
    }
  }

  async function sample() {
    if (inProgress) return inProgress;
    inProgress = (async () => {
      const at = Date.now(),
        services = {};
      await Promise.all(
        Object.entries(endpoints).map(async ([name, url]) => {
          try {
            const result = await call(
              context,
              "GET",
              url + "/actuator/prometheus",
            );
            if (result.status !== 200) throw new Error(`HTTP ${result.status}`);
            services[name] = fromMetrics(result.body);
            if (pids[name])
              try {
                const result = await exec(
                  "ps",
                  ["-p", pids[name], "-o", "rss="],
                  {timeout: 2000},
                );
                const rss = Number(result.stdout.trim());
                services[name].rssBytes = rss > 0 ? rss * 1024 : null;
              } catch {
                services[name].rssBytes = null;
                warnings.add(`${name}: RSS sampling failed`);
              }
          } catch (error) {
            services[name] = {error: error.message};
            warnings.add(`${name}: metric sampling failed`);
          }
        }),
      );
      const cpu = process.cpuUsage(),
        row = {
          at,
          services,
          generator: {
            rssBytes: process.memoryUsage().rss,
            cpuNs: (cpu.user + cpu.system) * 1000,
          },
        };
      samples.push(row);
      write("resources", row);
      return row;
    })();
    try {
      return await inProgress;
    } finally {
      inProgress = null;
    }
  }

  const loop = (async () => {
    while (running) {
      await sample();
      await sleep(1000);
    }
  })();
  const dockerLoop = (async () => {
    if (!config.dockerStats) return;
    try {
      const names = (
        await exec(
          "docker",
          [
            "ps",
            "--filter",
            "label=com.docker.compose.project=stock-trading",
            "--format",
            "{{.Names}}",
          ],
          {timeout: 5000},
        )
      ).stdout
        .trim()
        .split("\n")
        .filter(Boolean);
      if (!names.length) {
        warnings.add("No stock-trading Docker containers discovered");
        return;
      }
      while (running) {
        try {
          const result = await exec(
            "docker",
            ["stats", "--no-stream", "--format", "{{json .}}", ...names],
            {timeout: 8000, maxBuffer: 1024 * 1024},
          );
          const row = {
            at: Date.now(),
            containers: result.stdout
              .trim()
              .split("\n")
              .filter(Boolean)
              .map((line) => {
                const s = JSON.parse(line);
                return {
                  name: s.Name,
                  cpuCores: parseFloat(s.CPUPerc) / 100,
                  memoryBytes: bytes(s.MemUsage.split("/")[0]),
                };
              }),
          };
          docker.push(row);
          write("docker", row);
        } catch {
          warnings.add("Docker resource sampling failed");
        }
        for (let i = 0; i < 5 && running; i++) await sleep(1000);
      }
    } catch {
      warnings.add(
        "Docker CLI unavailable; infrastructure sizing not measured",
      );
    }
  })();
  return {
    samples,
    docker,
    warnings,
    pids,
    sample,
    async stop() {
      running = false;
      await Promise.all([loop, dockerLoop]);
      await context.dispose();
    },
  };
}
