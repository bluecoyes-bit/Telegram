import { spawnSync } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(fileURLToPath(import.meta.url));
const outDir = path.resolve(root, "dist");
const bin = path.join(root, "node_modules/esbuild-wasm/bin/esbuild");

await mkdir(outDir, { recursive: true });

const result = spawnSync(
  process.execPath,
  [
    bin,
    "src/main.jsx",
    "--bundle",
    `--outfile=${path.join(outDir, "app.js")}`,
    "--format=iife",
    "--jsx=automatic",
    "--loader:.jsx=jsx",
    "--minify",
    "--log-level=info",
  ],
  { cwd: root, stdio: "inherit" }
);

if (result.status !== 0) process.exit(result.status || 1);

const html = `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0" />
    <title>TG Gateway — Multi-account Control Center</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet" />
    <link rel="stylesheet" href="/dashboard/app.css" />
    <script src="/web-config.js"></script>
  </head>
  <body>
    <div id="root"></div>
    <script src="/dashboard/app.js"></script>
  </body>
</html>
`;
await writeFile(path.join(outDir, "index.html"), html);
console.log("dashboard built to frontend/dist/");
