#!/usr/bin/env node
// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// Generates provisa.dev's live demos page (REQ-2006): one card for each published named demo,
// written into site/demos.html between the `demos` marker comments, and each card's screenshot
// copied into site/assets under a name carrying its content hash (assets/* is served immutable).
//
//   node scripts/gen-site-demos.mjs            write the cards and the screenshots
//   node scripts/gen-site-demos.mjs --check    exit 1 when the page or a screenshot is out of step
//
// Source of truth: demo/named/<name>/demo.yaml -- title, description, screenshot (a file beside
// it), published (true or false), and optionally order (a positive integer). A demo with
// published: false has no card; a published one needs the first four. Cards with an order come
// first, lowest first; the rest follow by name. Its "Open live demo" button opens LIVE_DEMO_URL + <name>: the no-signup sandbox
// of that demo on cloud.provisa.dev, which the cloud deployment serves.
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(path.join(ROOT, "provisa-ui", "package.json"));
const yaml = require("js-yaml");

const NAMED = path.join(ROOT, "demo/named");
const PAGE = path.join(ROOT, "site/demos.html");
const ASSETS = path.join(ROOT, "site/assets");
const START = "<!-- demos:start -->";
const END = "<!-- demos:end -->";
/** Where a demo's sandbox is opened: its name follows. */
export const LIVE_DEMO_URL = "https://cloud.provisa.dev/demos/";

const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

/** Every named demo's demo.yaml, by name; refused by name when one is incomplete. */
export function readDemos() {
  const demos = [];
  for (const name of fs.readdirSync(NAMED).sort()) {
    const file = path.join(NAMED, name, "demo.yaml");
    if (!fs.existsSync(file)) continue;
    const spec = yaml.load(fs.readFileSync(file, "utf8")) ?? {};
    if (typeof spec.published !== "boolean") {
      throw new Error(`${path.relative(ROOT, file)}: published must be true or false`);
    }
    if (!spec.published) continue;
    for (const key of ["title", "description", "screenshot"]) {
      if (typeof spec[key] !== "string" || !spec[key].trim()) {
        throw new Error(`${path.relative(ROOT, file)}: a published demo needs a ${key}`);
      }
    }
    const shot = path.join(NAMED, name, spec.screenshot);
    if (!fs.existsSync(shot)) {
      throw new Error(`${path.relative(ROOT, file)}: screenshot ${spec.screenshot} is not beside it`);
    }
    if (spec.order !== undefined && !(Number.isInteger(spec.order) && spec.order > 0)) {
      throw new Error(`${path.relative(ROOT, file)}: order must be a positive integer`);
    }
    const order = spec.order ?? Infinity;
    demos.push({ name, title: spec.title.trim(), description: spec.description.trim(), shot, order });
  }
  // Sorted by name already: a stable sort keeps that order among demos with no order.
  return demos.sort((a, b) => a.order - b.order);
}

/** A screenshot as the site serves it: the file unchanged, its name carrying its content hash. */
function assetFor(demo) {
  const bytes = fs.readFileSync(demo.shot);
  const hash = crypto.createHash("sha256").update(bytes).digest("hex").slice(0, 10);
  return { bytes, name: `demo-${demo.name}.${hash}${path.extname(demo.shot).toLowerCase()}` };
}

export function render() {
  const demos = readDemos();
  const assets = demos.map(assetFor);
  const cards = demos.map((d, i) =>
    [
      `<article class="demo-card" data-demo="${esc(d.name)}">`,
      `  <img class="demo-shot" src="/assets/${assets[i].name}" alt="${esc(d.title)}: the demo in Provisa" loading="lazy" />`,
      `  <div class="demo-body">`,
      `    <h2>${esc(d.title)}</h2>`,
      `    <p>${esc(d.description)}</p>`,
      `    <a class="btn btn-primary" href="${LIVE_DEMO_URL}${encodeURIComponent(d.name)}" target="_blank" rel="noopener">Open live demo</a>`,
      `  </div>`,
      `</article>`,
    ].join("\n"),
  );
  const html = `<div class="demo-grid" data-count="${demos.length}">\n${cards.join("\n")}\n</div>`;
  return { html, assets, count: demos.length };
}

/** `page` with its marked region replaced by the cards; throws when the markers are missing. */
export function splice(page, html) {
  const a = page.indexOf(START);
  const b = page.indexOf(END);
  if (a === -1 || b === -1 || b < a) throw new Error("demos markers missing from site/demos.html");
  return page.slice(0, a + START.length) + "\n" + html + "\n" + page.slice(b);
}

const shotsOnDisk = () => fs.readdirSync(ASSETS).filter((f) => /^demo-[\w-]+\.[0-9a-f]{10}\.\w+$/.test(f));

/** What is out of step with the demo.yaml files: the page, and the screenshots on disk. */
export function stale() {
  const { html, assets } = render();
  const out = [];
  const cur = fs.readFileSync(PAGE, "utf8");
  if (splice(cur, html) !== cur) out.push("site/demos.html");
  const wanted = assets.map((a) => a.name).sort();
  const onDisk = shotsOnDisk().sort();
  const same =
    JSON.stringify(onDisk) === JSON.stringify(wanted) &&
    assets.every((a) => fs.readFileSync(path.join(ASSETS, a.name)).equals(a.bytes));
  if (!same) out.push(...wanted.map((n) => "site/assets/" + n));
  return out;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const { html, assets, count } = render();
  if (process.argv.includes("--check")) {
    const out = stale();
    if (out.length) {
      console.error(`out of step with demo/named/*/demo.yaml: ${out.join(", ")} (run scripts/gen-site-demos.mjs)`);
      process.exit(1);
    }
    console.log(`site live demos page in step (${count} published)`);
  } else {
    for (const f of shotsOnDisk()) fs.unlinkSync(path.join(ASSETS, f));
    for (const a of assets) fs.writeFileSync(path.join(ASSETS, a.name), a.bytes);
    fs.writeFileSync(PAGE, splice(fs.readFileSync(PAGE, "utf8"), html));
    console.log(`wrote ${count} published demo(s) into site/demos.html`);
  }
}
