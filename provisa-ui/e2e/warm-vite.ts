// Copyright (c) 2026 Kenneth Stott
// Canary: 5b0f6d0e-7c1a-4a5e-9d53-2f1c8e7a4b36
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// The Vite dev server transforms a module (TypeScript, JSX, Istanbul counters) the first time it
// is asked for it, and index.html names one module: fetching the page transforms nothing. Left to
// the tests, the first page load of each worker pays for the whole graph while the other worker
// does the same: run 43860857546, first test on each of 2 workers, `goto /sources` 26.4 s and
// the page header 15.7 s after it (1300 module requests, the dev server at 70-83% of a CPU for
// 90 s, each backend's 2 s schema rebuild taking 6-8 s beside it), and the 120 s test ran out.
//
// So the graph is walked here, once, before any worker starts: each module is fetched as a
// browser would fetch it, and the modules it imports (statically or by import()) are fetched in
// turn. A response is the module transformed, so when the walk ends every page of the app is
// served from the dev server's cache.

/** Vite's own pre-bundled dependencies: built when the server starts and served from disk. */
const PREBUNDLED = "/node_modules/.vite/";

// The dev server rewrites every specifier to a URL path, so a specifier is read as written.
// Static forms start a statement; `=` is excluded so `export const a = "..."` is not one.
const STATIC_FROM = /(?:^|[;}\n])[ \t]*(?:import|export)\b[^"'`;()=]*?\bfrom\s*["']([^"'\n]+)["']/g;
const STATIC_BARE = /(?:^|[;}\n])[ \t]*import\s*["']([^"'\n]+)["']/g;
const DYNAMIC = /\bimport\(\s*["']([^"'\n]+)["']\s*\)/g;

/** The URLs the module at `moduleUrl` imports, as the dev server wrote them into `code`. */
export function importedUrls(code: string, moduleUrl: string): string[] {
  const origin = new URL(moduleUrl).origin;
  const found = new Set<string>();
  for (const pattern of [STATIC_FROM, STATIC_BARE, DYNAMIC]) {
    for (const match of code.matchAll(pattern)) {
      const spec = match[1];
      if (!spec.startsWith("/") && !spec.startsWith(".")) continue;
      const url = new URL(spec, moduleUrl);
      if (url.origin !== origin || url.pathname.startsWith(PREBUNDLED)) continue;
      found.add(url.href);
    }
  }
  return [...found];
}

export type ModuleFetch = (
  url: string,
) => Promise<{ ok: boolean; status: number; contentType: string; text: () => Promise<string> }>;

const browserLikeFetch: ModuleFetch = async (url) => {
  const res = await fetch(url, { signal: AbortSignal.timeout(180000) });
  return {
    ok: res.ok,
    status: res.status,
    contentType: res.headers.get("content-type") ?? "",
    text: () => res.text(),
  };
};

/**
 * Fetch every module reachable from `entryPaths` on the dev server at `uiUrl`, `concurrency` at a
 * time. Returns how many modules were fetched. A module the server does not answer with 2xx fails
 * the walk, naming it and the module that imports it: a browser would fail on the same import.
 */
export async function warmViteModuleGraph(
  uiUrl: string,
  entryPaths: string[],
  fetchModule: ModuleFetch = browserLikeFetch,
  concurrency = 8,
): Promise<number> {
  const importerOf = new Map<string, string>();
  const queue: string[] = [];
  for (const entry of entryPaths) {
    const url = new URL(entry, uiUrl).href;
    importerOf.set(url, "(entry)");
    queue.push(url);
  }
  let inFlight = 0;
  let fetched = 0;
  await new Promise<void>((resolve, reject) => {
    let failed = false;
    const fail = (err: unknown) => {
      failed = true;
      reject(err);
    };
    const visit = async (url: string) => {
      const res = await fetchModule(url);
      if (!res.ok) {
        throw new Error(
          `Vite dev server answered ${res.status} for ${url}, imported by ${importerOf.get(url)}`,
        );
      }
      fetched += 1;
      if (!res.contentType.includes("javascript")) return;
      for (const next of importedUrls(await res.text(), url)) {
        if (importerOf.has(next)) continue;
        importerOf.set(next, url);
        queue.push(next);
      }
    };
    const pump = () => {
      if (failed) return;
      if (queue.length === 0 && inFlight === 0) return resolve();
      while (inFlight < concurrency && queue.length > 0) {
        const url = queue.shift() as string;
        inFlight += 1;
        visit(url).then(() => {
          inFlight -= 1;
          pump();
        }, fail);
      }
    };
    pump();
  });
  return fetched;
}
