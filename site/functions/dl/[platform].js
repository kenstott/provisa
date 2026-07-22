// Copyright (c) 2026 Kenneth Stott
// Canary: f870cd71-c98d-41e5-b4ba-57049911c122
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// GET /dl/:platform — 302 to the newest release asset for that platform.
//
// Unlike GitHub's /releases/latest/download/<name>, this resolves the newest release
// INCLUDING pre-releases, so the download links work during the preview phase (when
// every release is a pre-release and GitHub has no "Latest"). The link target
// (https://provisa.dev/dl/macos) never changes across releases — docs link to it once.

const REPO = "kenstott/provisa";

// name matchers per platform — match both versioned assets (Provisa-v0.1.0-alpha.1-macOS.dmg)
// and the version-less stable aliases (Provisa-macOS.dmg) the release workflow uploads.
const MATCHERS = {
  macos: (n) => /macOS\.dmg$/i.test(n) && !/(obs|demo|container)/i.test(n),
  "macos-obs": (n) => /Obs.*macOS\.dmg$/i.test(n),
  "macos-demo": (n) => /Demo.*macOS\.dmg$/i.test(n),
  linux: (n) => /linux.*\.AppImage$/i.test(n),
  windows: (n) => /windows-x64\.exe$/i.test(n) && !/container/i.test(n),
  "windows-container": (n) => /Container.*windows-x64\.exe$/i.test(n),
  jdbc: (n) => /^provisa-jdbc.*\.jar$/i.test(n),
};

export async function onRequestGet(context) {
  const platform = String(context.params.platform || "").toLowerCase();
  const match = MATCHERS[platform];
  if (!match) {
    return new Response(`Unknown platform '${platform}'`, { status: 404 });
  }

  const res = await fetch(`https://api.github.com/repos/${REPO}/releases?per_page=30`, {
    headers: {
      Accept: "application/vnd.github+json",
      "User-Agent": "provisa-dl-redirect",
    },
    // Cache the GitHub API response at the edge (5 min) so we don't exhaust the
    // unauthenticated 60 req/hr/IP limit under load.
    cf: { cacheTtl: 300, cacheEverything: true },
  });
  if (!res.ok) {
    return new Response("Release lookup failed", { status: 502 });
  }

  // GitHub returns releases newest-first; take the first (non-draft) one that carries a
  // matching asset — that is the newest build for this platform, pre-release or not.
  const releases = await res.json();
  for (const rel of releases) {
    if (rel.draft) continue;
    const asset = (rel.assets || []).find((a) => match(a.name));
    if (asset) {
      return Response.redirect(asset.browser_download_url, 302);
    }
  }
  return new Response("No matching release asset found", { status: 404 });
}
