import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";

const updaterBundles = [
  {
    platforms: ["darwin-aarch64", "darwin-aarch64-app"],
    suffix: "_aarch64.app.tar.gz",
  },
  {
    platforms: ["darwin-x86_64", "darwin-x86_64-app"],
    suffix: "_x64.app.tar.gz",
  },
  {
    platforms: ["windows-x86_64", "windows-x86_64-nsis"],
    suffix: "_x64-setup.exe",
  },
];

/** Build and validate the desktop updater manifest from uploaded release assets. */
export function buildUpdaterManifest(
  version,
  assets,
  signatures,
  publishedAt = new Date(),
) {
  const platforms = {};

  for (const bundle of updaterBundles) {
    const name = `EcoPaste_${version}${bundle.suffix}`;
    const asset = assets.find((candidate) => candidate.name === name);
    if (!asset?.url) {
      throw new Error(`Missing updater asset: ${name}`);
    }

    const signature = signatures[`${name}.sig`]?.trim();
    if (!signature) {
      throw new Error(`Missing updater signature: ${name}.sig`);
    }

    for (const platform of bundle.platforms) {
      platforms[platform] = { signature, url: asset.url };
    }
  }

  return {
    notes: "",
    platforms,
    pub_date: publishedAt.toISOString(),
    version,
  };
}

async function fetchOrThrow(url, options) {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`GitHub API request failed (${response.status}): ${url}`);
  }

  return response;
}

async function main() {
  const {
    GITHUB_API_URL: apiUrl,
    GH_REPO: repository,
    GH_TOKEN: token,
    RELEASE_ID: releaseId,
    RELEASE_VERSION: version,
    UPDATER_MANIFEST_PATH: outputPath,
  } = process.env;
  if (
    !apiUrl ||
    !repository ||
    !token ||
    !releaseId ||
    !version ||
    !outputPath
  ) {
    throw new Error("Updater manifest environment is incomplete");
  }

  const headers = {
    Accept: "application/vnd.github+json",
    Authorization: `Bearer ${token}`,
    "X-GitHub-Api-Version": "2022-11-28",
  };
  // shortcut: EcoPaste releases have fewer than 100 assets; paginate if that changes.
  const assetsResponse = await fetchOrThrow(
    `${apiUrl}/repos/${repository}/releases/${releaseId}/assets?per_page=100`,
    { headers },
  );
  const assets = await assetsResponse.json();
  const signatureNames = updaterBundles.map(
    (bundle) => `EcoPaste_${version}${bundle.suffix}.sig`,
  );
  const signatures = {};

  for (const name of signatureNames) {
    const asset = assets.find((candidate) => candidate.name === name);
    if (!asset?.url) {
      throw new Error(`Missing updater signature: ${name}`);
    }

    const response = await fetchOrThrow(asset.url, {
      headers: { ...headers, Accept: "application/octet-stream" },
    });
    signatures[name] = await response.text();
  }

  const manifest = buildUpdaterManifest(version, assets, signatures);
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, `${JSON.stringify(manifest, null, 2)}\n`);
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  await main();
}
