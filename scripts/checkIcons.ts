import {
  createIconManifest,
  iconOutputs,
  iconSources,
  readIconManifest,
} from "./iconAssets";

const problems: string[] = [];

try {
  const expected = await readIconManifest();
  const current = await createIconManifest();

  if (expected.tauriVersion !== current.tauriVersion) {
    problems.push(
      `Tauri CLI changed: ${expected.tauriVersion} -> ${current.tauriVersion}`,
    );
  }
  for (const name of Object.keys(iconSources) as (keyof typeof iconSources)[]) {
    if (expected.sources[name] !== current.sources[name]) {
      problems.push(`icon source changed: ${iconSources[name]}`);
    }
  }
  for (const output of iconOutputs) {
    if (
      expected.outputs[output.targetPath] !== current.outputs[output.targetPath]
    ) {
      problems.push(`generated icon changed: ${output.targetPath}`);
    }
  }
} catch (error) {
  problems.push(error instanceof Error ? error.message : String(error));
}

if (problems.length === 0) {
  process.stdout.write("Icon assets are up to date.\n");
} else {
  process.stderr.write(
    `Icon assets are stale:\n${problems.map((problem) => `- ${problem}`).join("\n")}\nRun "pnpm icons:generate" and commit the results.\n`,
  );
  process.exitCode = 1;
}
