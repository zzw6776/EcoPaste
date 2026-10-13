import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

const exec = promisify(execFile);
const workflow = await readFile(
  new URL("../.github/workflows/release.yml", import.meta.url),
  "utf8",
);

/** Extract the actual inline workflow commands for isolated execution. */
function workflowRun(name) {
  const step = workflow.split(`      - name: ${name}\n`)[1];
  assert.ok(step, `Missing workflow step ${name}`);
  const lines = [];

  for (const line of step.split("        run: |\n")[1].split("\n")) {
    if (line.trim() && !line.startsWith("          ")) break;
    lines.push(line.slice(10));
  }

  return lines.join("\n");
}

const validation = workflowRun("Validate release notes")
  .split("<<'NODE'\n")[1]
  .split("\nNODE")[0];
const preparation = workflowRun("Validate version and create draft release");
const publication = workflowRun("Publish completed release");

/** Create disposable version files without accessing the real release CLI. */
async function fixture(
  t,
  notes = "# 1.1.141\n\n- 更新说明使用简洁中文，便于查看版本变化。\n",
  version = "1.1.141",
) {
  const directory = await mkdtemp(join(tmpdir(), "ecopaste-release-notes-"));
  t.after(async () => {
    await rm(directory, { force: true, recursive: true });
  });
  await mkdir(join(directory, "docs", "releases"), { recursive: true });
  await mkdir(join(directory, "src-tauri"));
  await writeFile(join(directory, "package.json"), JSON.stringify({ version }));
  await writeFile(
    join(directory, "src-tauri", "Cargo.toml"),
    `[package]\nname = "EcoPaste"\nversion = "${version}"\n`,
  );
  await writeFile(
    join(directory, "src-tauri", "Cargo.lock"),
    `[[package]]\nname = "EcoPaste"\nversion = "${version}"\n`,
  );
  if (notes != null) {
    await writeFile(
      join(directory, "docs", "releases", `${version}.md`),
      notes,
    );
  }

  return directory;
}

test("发布校验接受当前中文日志、Windows 换行和预发布版本", async (t) => {
  const { version } = JSON.parse(
    await readFile(new URL("../package.json", import.meta.url), "utf8"),
  );
  const notes = await readFile(
    new URL(`../docs/releases/${version}.md`, import.meta.url),
    "utf8",
  );

  for (const content of [notes, notes.replace(/\n/g, "\r\n")]) {
    const directory = await fixture(t, content, version);
    const result = await exec(
      process.execPath,
      ["--input-type=module", "-e", validation],
      { cwd: directory },
    );
    assert.ok(result.stdout.includes(`Validated docs/releases/${version}.md`));
    assert.equal(
      await readFile(
        join(directory, "docs", "releases", `${version}.md`),
        "utf8",
      ),
      content,
    );
  }

  const beta = await fixture(
    t,
    "# 1.1.141-beta.1\n- 改进测试版发布说明。\n",
    "1.1.141-beta.1",
  );
  await exec(process.execPath, ["--input-type=module", "-e", validation], {
    cwd: beta,
  });
});

test("发布校验拒绝缺失、空白、错误版本和只有 Full Changelog 的正文", async (t) => {
  for (const notes of [
    null,
    "",
    " \n",
    "# 1.1.140\n- 改进发布说明。",
    "# 1.1.141\n",
    "# 1.1.141\n-   \n",
    "# 1.1.141\n- Full Changelog: https://example.test/compare",
  ]) {
    const directory = await fixture(t, notes);
    await assert.rejects(
      exec(process.execPath, ["--input-type=module", "-e", validation], {
        cwd: directory,
      }),
      (error) => {
        return (
          error.code === 1 &&
          /ENOENT|first line|Chinese change entry/.test(error.stderr)
        );
      },
    );
  }
});

/** Run production Bash with fake git/gh functions; never tag, push or publish. */
async function runRelease(t, script, state = "new") {
  const directory = await fixture(t);
  const callsPath = join(directory, "calls.txt");
  const mock = `
    git() {
      case "$1" in
        show) printf '{"version":"1.1.140"}\\n';;
        rev-list) printf 'fixture-sha\\n';;
      esac
      return 0
    }
    gh() {
      printf '%s\\t' "$@" >> "$TEST_RELEASE_CALLS"
      printf '\\n' >> "$TEST_RELEASE_CALLS"
      if [[ "$2" == view ]]; then
        case "$*" in
          *'--json isDraft'*)
            if [[ "$TEST_RELEASE_STATE" == published ]]; then printf 'false\\n'; else printf 'true\\n'; fi;;
          *'--json databaseId'*) printf '123\\n';;
          *) if [[ "$TEST_RELEASE_STATE" == new ]]; then return 1; fi;;
        esac
        return 0
      fi
      while (( $# )); do
        if [[ "$1" == --notes-file ]]; then cp "$2" published-notes.md; return $?; fi
        shift
      done
    }
  `;
  let failure = null;

  try {
    await exec("bash", ["-c", `${mock}\n${script}`], {
      cwd: directory,
      env: {
        ...process.env,
        GITHUB_OUTPUT: join(directory, "outputs.txt"),
        GITHUB_SHA: "fixture-sha",
        PRERELEASE: "false",
        RELEASE_VERSION: "1.1.141",
        TAG_NAME: "v1.1.141",
        TEST_RELEASE_CALLS: callsPath,
        TEST_RELEASE_STATE: state,
      },
    });
  } catch (error) {
    failure = error;
  }

  let output = "";
  try {
    output = await readFile(callsPath, "utf8");
  } catch {}
  const calls = output
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      return line.split("\t").slice(0, -1);
    });

  return { calls, directory, failure };
}

for (const state of ["new", "draft"]) {
  test(`${state === "new" ? "新建" : "复用"}草稿时使用完整版本正文`, {
    skip: process.platform === "win32",
  }, async (t) => {
    const { calls, directory, failure } = await runRelease(
      t,
      preparation,
      state,
    );
    assert.equal(failure, null);
    const writes = calls.filter((args) => {
      return args[1] !== "view";
    });
    assert.equal(writes.length, 1);
    assert.equal(writes[0][1], state === "new" ? "create" : "edit");
    assert.equal(
      writes[0][writes[0].indexOf("--notes-file") + 1],
      "docs/releases/1.1.141.md",
    );
    assert.ok(!writes[0].includes("--generate-notes"));
    assert.equal(
      await readFile(join(directory, "published-notes.md"), "utf8"),
      await readFile(join(directory, "docs", "releases", "1.1.141.md"), "utf8"),
    );
  });
}

test("已发布版本仍拒绝覆盖", {
  skip: process.platform === "win32",
}, async (t) => {
  const { calls, failure } = await runRelease(t, preparation, "published");
  assert.equal(failure?.code, 1);
  assert.match(failure.stdout, /already published/);
  assert.equal(
    calls.every((args) => {
      return args[1] === "view";
    }),
    true,
  );
});

test("完成发布时再次原样写入版本正文", {
  skip: process.platform === "win32",
}, async (t) => {
  const { calls, directory, failure } = await runRelease(
    t,
    publication,
    "draft",
  );
  assert.equal(failure, null);
  assert.equal(calls.length, 1);
  assert.equal(calls[0][1], "edit");
  assert.ok(calls[0].includes("--draft=false"));
  assert.ok(calls[0].includes("--prerelease=false"));
  assert.equal(
    calls[0][calls[0].indexOf("--notes-file") + 1],
    "docs/releases/1.1.141.md",
  );
  assert.equal(
    await readFile(join(directory, "published-notes.md"), "utf8"),
    await readFile(join(directory, "docs", "releases", "1.1.141.md"), "utf8"),
  );
});
