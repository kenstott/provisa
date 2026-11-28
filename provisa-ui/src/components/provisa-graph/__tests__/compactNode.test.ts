// Copyright (c) 2026 Kenneth Stott
// Canary: 3b8e5d21-7c4f-4a90-b6e2-1f9d0a7c5e48
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-28, THE SHARED NODE STYLE IS COMPACT): every graph the shared component
// draws spends a node's space on its content. No icon and no type row; the type's colour a thin
// edge; the name the bold first row; the body's rows small and tight; long text ellipsised, whole
// on hover; the box as wide as its text up to a cap.

import { describe, expect, it } from "vitest";
import cytoscape from "cytoscape";
import {
  NODE_MAX_WIDTH,
  TEXT_LEFT,
  TEXT_RIGHT,
  ellipsise,
  graphElements,
  nameWidth,
  nodeHeight,
  rowWidth,
  type GraphSpec,
} from "../model";
import { cardImage, graphStylesheet } from "../stylesheet";
import { SIZE, graphTokens } from "../tokens";

const LONG_CAP = "query:finance.general_ledger_entries_by_cost_centre_and_region_by_month";
const SPEC: GraphSpec = {
  nodes: [
    {
      id: "dq_steward",
      type: "role",
      name: "dq_steward",
      lines: [{ text: "standard role" }, { text: "dq_rw" }, { text: "dq_expectations_write" }],
    },
    { id: "viewer", type: "role", name: "viewer" },
    {
      id: "long",
      type: "role",
      name: "meta:emea_finance_reviewers_and_regional_controllers_group",
      lines: [{ text: "meta-role" }, { text: LONG_CAP }],
    },
    { id: "j", type: "junction", name: "pet_companions" },
    { id: "more", type: "more", kind: "more", name: "+3 columns" },
  ],
  edges: [{ id: "e", source: "viewer", target: "dq_steward", arrow: "target" }],
};

const data = (id: string) =>
  graphElements(SPEC).find((e) => e.data.id === id)?.data as Record<string, unknown>;

const drawn = () =>
  cytoscape({
    headless: true,
    styleEnabled: true,
    elements: graphElements(SPEC) as unknown as Parameters<typeof cytoscape>[0]["elements"],
    style: graphStylesheet(graphTokens(false)) as unknown as Parameters<
      typeof cytoscape
    >[0]["style"],
  });

describe("a compact box", () => {
  it("has no header row: its name, then its body, and no type name", () => {
    const d = data("dq_steward");
    expect(d.label).toBe("dq_steward\nstandard role\ndq_rw\ndq_expectations_write");
    expect(String(d.label)).not.toMatch(/ROLE\b/);
    expect(d.rows).toEqual(["standard role", "dq_rw", "dq_expectations_write"]);
    expect(d.h).toBe(nodeHeight(3));
    expect(d.h).toBe(2 * SIZE.padY + SIZE.nameStep + SIZE.bodyGap + 3 * SIZE.lineStep);
    // A box with its name only is one row high.
    expect(data("viewer").h).toBe(SIZE.nodeHeight);
  });

  it("draws no icon and no cytoscape label: its text is its card, the band a thin edge", () => {
    const cy = drawn();
    const n = cy.getElementById("dq_steward");
    const image = String(n.style("background-image"));
    const band = String(n.style("background-gradient-stop-positions"));
    const caption = n.data("caption");
    cy.destroy();
    expect(caption).toBe("");
    // One image, the card; no icon beside it.
    expect(image.startsWith("data:image/svg+xml")).toBe(true);
    const svg = decodeURIComponent(image.slice(image.indexOf(",") + 1));
    expect(svg).toContain(">dq_steward</text>");
    expect(svg).not.toContain("<path");
    expect(SIZE.band).toBeLessThanOrEqual(3);
    expect(band).toContain(`${((SIZE.band / (data("dq_steward").w as number)) * 100).toFixed(2)}%`);
  });

  it("draws its name bold on the first row and its body's rows smaller under it", () => {
    const tokens = graphTokens(false);
    const d = data("dq_steward");
    const svg = decodeURIComponent(cardImage({ data: (k: string) => d[k] }, tokens).split(",")[1]);
    const texts = [...svg.matchAll(/<text ([^>]*)>([^<]*)<\/text>/g)].map((m) => ({
      attrs: m[1],
      text: m[2],
      y: Number(/ y='([\d.]+)'/.exec(m[1])?.[1]),
      size: Number(/font-size='([\d.]+)'/.exec(m[1])?.[1]),
    }));
    expect(texts.map((t) => t.text)).toEqual([
      "dq_steward",
      "standard role",
      "dq_rw",
      "dq_expectations_write",
    ]);
    expect(texts[0].attrs).toContain("font-weight='600'");
    expect(texts[0].size).toBe(SIZE.name);
    for (const row of texts.slice(1)) {
      expect(row.size).toBeLessThan(SIZE.name);
      expect(row.attrs).not.toContain("font-weight");
    }
    // Tight: each row a line step below the last, all from the same left.
    for (let i = 2; i < texts.length; i++) expect(texts[i].y - texts[i - 1].y).toBe(SIZE.lineStep);
    for (const t of texts) expect(t.attrs).toContain(`x='${TEXT_LEFT}'`);
    expect(texts[texts.length - 1].y + SIZE.lineStep / 2 + SIZE.padY).toBe(d.h);
  });

  it("escapes what it draws", () => {
    const d = { ...data("viewer"), title: "a<b & 'c'" };
    const svg = decodeURIComponent(
      cardImage({ data: (k: string) => d[k as keyof typeof d] }, graphTokens(true)).split(",")[1],
    );
    expect(svg).toContain("a&lt;b &amp; &apos;c&apos;");
  });

  it("is as wide as its text needs, from the least width up to the cap", () => {
    const viewer = data("viewer");
    const steward = data("dq_steward");
    expect(viewer.w).toBe(SIZE.nodeWidth);
    expect(steward.w).toBe(Math.ceil(TEXT_LEFT + rowWidth("dq_expectations_write") + TEXT_RIGHT));
    expect(steward.w).toBeLessThan(NODE_MAX_WIDTH);
    expect(data("long").w).toBe(NODE_MAX_WIDTH);
  });

  it("ellipsises a name and a row past the cap, never wrapping, and keeps them whole for hover", () => {
    const d = data("long");
    const room = d.textW as number;
    const [name, ...rows] = String(d.label).split("\n");
    expect(name.endsWith("…")).toBe(true);
    expect(nameWidth(name)).toBeLessThanOrEqual(room);
    expect(rows[0]).toBe("meta-role");
    expect(rows[1].endsWith("…")).toBe(true);
    expect(rowWidth(rows[1])).toBeLessThanOrEqual(room);
    // Two rows: the box does not grow for what it cut.
    expect(d.h).toBe(nodeHeight(2));
    expect(d.clipped).toEqual([
      "meta:emea_finance_reviewers_and_regional_controllers_group",
      LONG_CAP,
    ]);
    // The default card names the node by its whole name.
    expect(d.name).toBe("meta:emea_finance_reviewers_and_regional_controllers_group");
    expect(data("dq_steward").clipped).toEqual([]);
  });

  it("takes about half the area the box with an icon and a type row took", () => {
    // Before: at least 180 wide, 13px a line over its name, its type, a rule and its body, + 14.
    const before = 180 * (6 * 13 + 14);
    const d = data("dq_steward");
    expect((d.w as number) * (d.h as number)).toBeLessThanOrEqual(0.55 * before);
  });
});

describe("a compact diamond and fold node", () => {
  it("draw their name only, centred, on one row", () => {
    const cy = drawn();
    const j = cy.getElementById("j");
    const more = cy.getElementById("more");
    const got = {
      j: [j.style("label"), j.style("text-halign"), j.style("text-wrap"), j.data("w"), j.data("h")],
      more: [more.style("label"), more.style("text-halign"), more.data("h")],
      jImage: String(j.style("background-image")),
    };
    cy.destroy();
    expect(got.j).toEqual([
      "pet_companions",
      "center",
      "none",
      SIZE.diamondWidth,
      SIZE.diamondHeight,
    ]);
    expect(got.more).toEqual(["+3 columns", "center", SIZE.nodeHeight]);
    expect(got.jImage).not.toContain("data:image");
  });
});

describe("ellipsise", () => {
  const w = (t: string) => t.length * 10;
  it("keeps text that fits and cuts text that does not to its room, an ellipsis at its end", () => {
    expect(ellipsise("abc", 30, w)).toBe("abc");
    expect(ellipsise("abcdef", 40, w)).toBe("abc…");
    expect(w(ellipsise("abcdef", 40, w))).toBeLessThanOrEqual(40);
  });
});
