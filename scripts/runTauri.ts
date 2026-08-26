import { spawnSync } from "node:child_process";
import { homedir } from "node:os";
import { delimiter, resolve } from "node:path";

const pathKey =
  Object.keys(process.env).find((key) => {
    return key.toLowerCase() === "path";
  }) ?? "PATH";
const cargoHome = process.env.CARGO_HOME ?? resolve(homedir(), ".cargo");
const cargoBin = resolve(cargoHome, "bin");
const result = spawnSync("tauri", process.argv.slice(2), {
  env: {
    ...process.env,
    [pathKey]: `${cargoBin}${delimiter}${process.env[pathKey] ?? ""}`,
  },
  shell: process.platform === "win32",
  stdio: "inherit",
});

if (result.error) {
  throw result.error;
}

process.exitCode = result.status ?? 1;
