#!/usr/bin/env node
// Generates the marketing site's rendering of the product's source-type picker and writes it into
// site/index.html and site/why/sources.html between the `source-picker` marker comments, and writes
// the counts it makes ("65 connectors", "80+ kinds of source") wherever a page or document states
// them (COUNTED).
//
//   node scripts/gen-site-source-picker.mjs            write the fragment and every stated count
//   node scripts/gen-site-source-picker.mjs --check    exit 1 when a page or a count is out of step
//
// Source of truth: provisa-ui/src/pages/sources/constants.ts (SOURCE_TYPES: order, labels,
// categories, the hosted Postgres services in `managed`) and provisa-ui/src/components/sourceLogoMarks.ts
// (which types have a published simple-icons mark, which a bundled logo file under
// provisa-ui/src/assets/source-logos, copied to site/assets; the rest are lettered tiles in the
// category's colour). REQUIREMENT_ONLY lists what the requirements add that the picker does not have yet;
// SUB_ITEMS lists the kinds of source a single tile reaches. Each entry names where it comes from.
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const UI = path.join(ROOT, "provisa-ui");
const require = createRequire(path.join(UI, "package.json"));

// Source types the requirements name that the picker does not list yet (target state).
export const REQUIREMENT_ONLY = [];

// The kinds of source reached through one tile, and where each list comes from.
export const SUB_ITEMS = {
  files: {
    from: "docs/sources.md (File Crawler formats table)",
    items: ["CSV", "TSV", "JSON", "YAML", "Excel (XLS, XLSX)", "Parquet", "Arrow", "HTML", "Markdown", "DOCX", "PPTX"],
    // Formats that are also tiles of their own, so they are not counted twice.
    alsoTiles: ["CSV", "Parquet"],
  },
};

// Categories shown first, in this order; the rest follow in the picker's own order.
const CATEGORY_FIRST = ["Enterprise"];

const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

async function loadSourceTypes() {
  const ts = require("typescript");
  const src = fs.readFileSync(path.join(UI, "src/pages/sources/constants.ts"), "utf8");
  const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } }).outputText;
  const mod = await import("data:text/javascript;base64," + Buffer.from(js).toString("base64"));
  return mod.SOURCE_TYPES;
}

// Each category's heading, as the product's picker shows it in English (the id is its key there).
function loadCategoryLabels() {
  const cat = JSON.parse(fs.readFileSync(path.join(UI, "src/i18n/locales/en/sourceTypePicker.json"), "utf8"));
  return cat.sourceTypePicker.categories;
}

function loadMarks() {
  const simple = require("simple-icons");
  const logo = fs.readFileSync(path.join(UI, "src/components/sourceLogoMarks.ts"), "utf8");
  const marks = {};
  // A mark's line may end in a comment naming why (google_workspace's does).
  for (const m of logo.matchAll(/^\s+(\w+): (si\w+),(\s*\/\/.*)?$/gm)) marks[m[1]] = simple[m[2]];
  // Bundled logo files: the import names the file, LOGO_FILES maps a type to the import.
  const imports = {};
  for (const m of logo.matchAll(/^import (\w+) from "\.\.\/assets\/source-logos\/([\w-]+\.svg)";$/gm)) {
    imports[m[1]] = m[2];
  }
  const files = {};
  const filesBlock = /const LOGO_FILES[^{]*\{([\s\S]*?)\n\};/.exec(logo)[1];
  for (const m of filesBlock.matchAll(/^\s+(\w+): (\w+),(\s*\/\/.*)?$/gm)) {
    if (!imports[m[2]]) throw new Error(`LOGO_FILES.${m[1]} names ${m[2]}, which imports no file`);
    files[m[1]] = imports[m[2]];
  }
  const tones = {};
  const block = /const CATEGORY_TONES[^{]*\{([\s\S]*?)\n\};/.exec(logo)[1];
  for (const m of block.matchAll(/^\s+"?([\w ]+?)"?: "(#[0-9a-f]{6})",$/gim)) tones[m[1]] = m[2];
  return { marks, tones, files };
}

function initials(label) {
  const words = label.replace(/\(.*?\)/g, "").trim().split(/[\s_-]+/).filter(Boolean);
  if (words.length >= 2) return (words[0][0] + words[1][0]).toUpperCase();
  return label.slice(0, 2).toUpperCase();
}

/** The picker's plain name for a tile: the label without a parenthetical suffix. */
const plainName = (s) => s.label.replace(/\s*\(.*\)\s*$/, "") || s.label;

/** The marks used, one <symbol> per simple-icons mark, as a file the pages share and the browser
    caches (assets/* is served immutable, so the name carries a hash of the content). */
function spriteFor(used) {
  const symbols = [...used.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([slug, m]) => `<symbol id="m-${slug}" viewBox="0 0 24 24"><path fill="#${m.hex}" d="${m.path}"/></symbol>`)
    .join("");
  const svg = `<svg xmlns="http://www.w3.org/2000/svg">${symbols}</svg>\n`;
  const hash = crypto.createHash("sha256").update(svg).digest("hex").slice(0, 10);
  return { svg, name: `source-marks.${hash}.svg` };
}

/** A bundled logo as the site serves it: the file unchanged, its name carrying its content hash. */
function logoFileFor(file) {
  const svg = fs.readFileSync(path.join(UI, "src/assets/source-logos", file), "utf8");
  const hash = crypto.createHash("sha256").update(svg).digest("hex").slice(0, 10);
  return { svg, name: `source-logo-${file.replace(/\.svg$/, "")}.${hash}.svg` };
}

export async function renderPicker() {
  const types = [...(await loadSourceTypes())];
  for (const add of REQUIREMENT_ONLY) {
    const at = types.findIndex((t) => t.value === add.after);
    types.splice(at + 1, 0, { value: add.value, label: add.label, category: add.category });
  }
  const { marks, tones, files } = loadMarks();
  const labels = loadCategoryLabels();
  const order = [...new Set(types.map((t) => t.category))];
  const cats = [...CATEGORY_FIRST.filter((c) => order.includes(c)), ...order.filter((c) => !CATEGORY_FIRST.includes(c))];
  const used = new Map();
  for (const t of types) if (marks[t.value]) used.set(marks[t.value].slug, marks[t.value]);
  const sprite = spriteFor(used);
  const logoFiles = new Map();
  for (const t of types) if (files[t.value]) logoFiles.set(t.value, logoFileFor(files[t.value]));
  const out = [];
  let total = 0;
  let extra = 0; // kinds of source shown under a tile that are not connectors of their own
  for (const cat of cats) {
    const rows = [];
    for (const s of types.filter((t) => t.category === cat)) {
      total += 1;
      const name = plainName(s);
      const mark = marks[s.value];
      const file = logoFiles.get(s.value);
      const logo = file
        ? `<img class="picker-mark" src="/assets/${file.name}" width="20" height="20" alt="" aria-hidden="true">`
        : mark
        ? `<svg class="picker-mark" width="20" height="20" aria-hidden="true"><use href="/assets/${sprite.name}#m-${mark.slug}"/></svg>`
        : `<span class="picker-tile" style="background:${tones[cat] ?? "#6b7280"}" aria-hidden="true">${esc(initials(s.label))}</span>`;
      const subs = s.managed ? s.managed.split(/,\s*/) : (SUB_ITEMS[s.value]?.items ?? []);
      extra += s.managed ? subs.length : subs.filter((x) => !(SUB_ITEMS[s.value]?.alsoTiles ?? []).includes(x)).length;
      const sub = subs.length ? `<ul class="picker-sub">${subs.map((x) => `<li>${esc(x)}</li>`).join("")}</ul>` : "";
      rows.push(`<li class="picker-row">${logo}<span class="picker-name">${esc(name)}</span>${sub}</li>`);
    }
    if (!labels[cat]) throw new Error(`no English label for source category ${cat} in sourceTypePicker.json`);
    out.push(`<section class="picker-cat" data-category="${esc(cat)}"><h3>${esc(labels[cat])}</h3><ul>${rows.join("")}</ul></section>`);
  }
  // The larger number is a floor to the nearest five: connectors plus the kinds shown under tiles.
  const kinds = Math.floor((total + extra) / 5) * 5;
  return {
    html: `<div class="picker" data-count="${total}" data-kinds="${kinds}">${out.join("")}</div>`,
    total,
    kinds,
    sprite,
    logoFiles: [...logoFiles.values()],
  };
}

const PAGES = ["site/index.html", "site/why/sources.html"];
const START = "<!-- source-picker:start -->";
const END = "<!-- source-picker:end -->";

/** `page` with its marked region replaced by the fragment; throws when the markers are missing. */
export function splice(page, html) {
  const a = page.indexOf(START);
  const b = page.indexOf(END);
  if (a === -1 || b === -1 || b < a) throw new Error("source-picker markers missing");
  return page.slice(0, a + START.length) + "\n" + html + "\n" + page.slice(b);
}

// Every file that states the number of connectors or of kinds of source.
export const COUNTED = [
  "README.md",
  "docs/competitive_analysis.md",
  "docs/marketing.md",
  "mkdocs.yml",
  "site/index.html",
  "site/vs/hasura.html",
  "site/why/data-mesh.html",
  "site/why/one-model.html",
  "site/why/sources.html",
];

const ONES = ["", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"];
const TENS = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"];
const ORDINAL_ONES = ["", "first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth"];
const ORDINAL_TENS = ["", "", "twentieth", "thirtieth", "fortieth", "fiftieth", "sixtieth", "seventieth", "eightieth", "ninetieth"];
function inRange(n) {
  if (n < 20 || n > 99) throw new Error(`the site writes counts of 20 to 99 in words, not ${n}`);
}
/** A number from 20 to 99 in words, as the site writes it ("sixty-four"). */
const inWords = (n) => (inRange(n), TENS[Math.floor(n / 10)] + (n % 10 ? "-" + ONES[n % 10] : ""));
/** Its ordinal ("sixty-fourth"). */
const inOrdinal = (n) =>
  (inRange(n), n % 10 ? TENS[Math.floor(n / 10)] + "-" + ORDINAL_ONES[n % 10] : ORDINAL_TENS[n / 10]);
const capital = (s) => s[0].toUpperCase() + s.slice(1);

/** `text` with every stated count made the picker's: the connectors, the kinds of source, the
    anchor and heading of the full list, "the other N" beside the one connector set apart, and the
    counts the Sources page spells out ("Seventy-two connectors, and one of them …", "Seventy-one
    of them connect to …", "The seventy-second is a crawler"). */
export function withCounts(text, total, kinds) {
  return text
    .replace(/\b[A-Z][a-z]+(?:-[a-z]+)? connectors, and one of them\b/g, `${capital(inWords(total))} connectors, and one of them`)
    .replace(/\b[A-Z][a-z]+(?:-[a-z]+)? of them connect to\b/g, `${capital(inWords(total - 1))} of them connect to`)
    .replace(/\bThe [a-z]+(?:-[a-z]+)?(\s+)is a crawler\b/g, (_m, gap) => `The ${inOrdinal(total)}${gap}is a crawler`)
    .replace(/\b\d+(\s+)connectors\b/g, (_m, gap) => `${total}${gap}connectors`)
    .replace(/\b\d+\+ kinds of source\b/g, `${kinds}+ kinds of source`)
    .replace(/#the-\d+"/g, `#the-${total}"`)
    .replace(/id="the-\d+">The \d+</g, `id="the-${total}">The ${total}<`)
    .replace(/unlike the other [a-z]+(-[a-z]+)?/g, `unlike the other ${inWords(total - 1)}`);
}

/** The files of COUNTED whose stated counts are not the picker's. */
export async function staleCounts() {
  const { total, kinds } = await renderPicker();
  return COUNTED.filter((f) => {
    const cur = fs.readFileSync(path.join(ROOT, f), "utf8");
    return withCounts(cur, total, kinds) !== cur;
  });
}

const ASSETS = path.join(ROOT, "site/assets");
const spritesOnDisk = () => fs.readdirSync(ASSETS).filter((f) => /^source-marks\.[0-9a-f]{10}\.svg$/.test(f));
const logoFilesOnDisk = () => fs.readdirSync(ASSETS).filter((f) => /^source-logo-[\w-]+\.[0-9a-f]{10}\.svg$/.test(f));

export async function stalePages() {
  const { html, sprite, logoFiles } = await renderPicker();
  const stale = PAGES.filter((p) => {
    const cur = fs.readFileSync(path.join(ROOT, p), "utf8");
    return splice(cur, html) !== cur;
  });
  const onDisk = spritesOnDisk();
  const spriteOk =
    onDisk.length === 1 && onDisk[0] === sprite.name && fs.readFileSync(path.join(ASSETS, sprite.name), "utf8") === sprite.svg;
  if (!spriteOk) stale.push("site/assets/" + sprite.name);
  const onDiskLogos = logoFilesOnDisk().sort();
  const wanted = logoFiles.map((f) => f.name).sort();
  const logosOk =
    JSON.stringify(onDiskLogos) === JSON.stringify(wanted) &&
    logoFiles.every((f) => fs.readFileSync(path.join(ASSETS, f.name), "utf8") === f.svg);
  if (!logosOk) stale.push(...wanted.map((n) => "site/assets/" + n));
  return stale;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const { html, total, kinds, sprite, logoFiles } = await renderPicker();
  if (process.argv.includes("--check")) {
    const stale = [...(await stalePages()), ...(await staleCounts())];
    if (stale.length) {
      console.error(`out of step with constants.ts: ${stale.join(", ")} (run scripts/gen-site-source-picker.mjs)`);
      process.exit(1);
    }
    console.log(`site source picker in step (${total} tiles)`);
  } else {
    for (const f of spritesOnDisk()) fs.unlinkSync(path.join(ASSETS, f));
    fs.writeFileSync(path.join(ASSETS, sprite.name), sprite.svg);
    for (const f of logoFilesOnDisk()) fs.unlinkSync(path.join(ASSETS, f));
    for (const f of logoFiles) fs.writeFileSync(path.join(ASSETS, f.name), f.svg);
    for (const p of PAGES) {
      const file = path.join(ROOT, p);
      fs.writeFileSync(file, splice(fs.readFileSync(file, "utf8"), html));
    }
    for (const f of COUNTED) {
      const file = path.join(ROOT, f);
      fs.writeFileSync(file, withCounts(fs.readFileSync(file, "utf8"), total, kinds));
    }
    console.log(`wrote ${total} tiles (${kinds}+ kinds) into ${PAGES.join(", ")}`);
  }
}
