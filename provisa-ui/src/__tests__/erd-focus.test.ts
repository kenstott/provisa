// Copyright (c) 2026 Kenneth Stott
// Canary: 0b7e41d2-6c35-4f9a-8d12-3a5e9c7f1b64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2187: a focus as a link: what the two lists say, what they come to against the tables, and
// its highlight on the diagram. A table the diagram does not hold is reported, never replaced.

import { describe, expect, it, vi } from "vitest";
import type { CyInstance } from "../components/graph/cytoscape-types";
import {
  addTableToFocus,
  erdSearch,
  focusIsOn,
  focusOnTable,
  highlightErdTables,
  readErdFocus,
  resolveErdFocus,
  writeErdFocus,
} from "../components/erd/erd-focus";
import type { ListedTable } from "../types/admin";

const table = (id: number, tableName: string, domainId: string) =>
  ({ id, tableName, domainId }) as ListedTable;
const TABLES = [
  table(1, "orders", "sales"),
  table(2, "customers", "sales"),
  table(3, "ledger", "finance"),
];
const DOMAINS = ["sales", "finance", "empty"];

function fakeCy(ids: string[], zoomed = 1) {
  const classed = new Set<string>();
  const fit = vi.fn();
  const center = vi.fn();
  const zoom = vi.fn((level?: number) => (level === undefined ? zoomed : undefined));
  const cy = {
    $: () => ({
      removeClass: () => classed.clear(),
      empty: () => classed.size === 0,
    }),
    getElementById: (id: string) => ({
      empty: () => !ids.includes(id),
      addClass: () => classed.add(id),
    }),
    fit,
    center,
    zoom,
  };
  return { cy: cy as unknown as CyInstance, classed, fit, center, zoom };
}

describe("readErdFocus", () => {
  it("reads both lists, each name once, and nothing from a link without them", () => {
    expect(readErdFocus(new URLSearchParams("domains=a,b,a&objects=a.x,,a.y"))).toEqual({
      domains: ["a", "b"],
      objects: ["a.x", "a.y"],
    });
    expect(readErdFocus(new URLSearchParams("view=erd"))).toEqual({ domains: [], objects: [] });
  });

  it("is on with either list", () => {
    expect(focusIsOn({ domains: [], objects: [] })).toBe(false);
    expect(focusIsOn({ domains: ["a"], objects: [] })).toBe(true);
    expect(focusIsOn({ domains: [], objects: ["a.x"] })).toBe(true);
  });
});

describe("writeErdFocus", () => {
  it("keeps the rest of the link, drops an empty list, and writes commas plain", () => {
    const params = new URLSearchParams("view=erd&table=4&domains=old");
    writeErdFocus(params, { domains: [], objects: ["sales.orders", "sales.x y"] });
    expect(erdSearch(params)).toBe("?view=erd&table=4&objects=sales.orders,sales.x+y");
  });
});

describe("resolveErdFocus", () => {
  it("is no focus without either list: the page's filter decides", () => {
    expect(resolveErdFocus({ domains: [], objects: [] }, TABLES, DOMAINS)).toEqual({
      shownDomains: null,
      domains: [],
      tables: [],
      highlighted: [],
      missing: [],
    });
  });

  it("shows only the objects' own domains when only objects are given", () => {
    const r = resolveErdFocus({ domains: [], objects: ["finance.ledger"] }, TABLES, DOMAINS);
    expect(r.shownDomains).toEqual(new Set(["finance"]));
    expect(r.highlighted).toEqual([3]);
    expect(r.domains).toEqual(["finance"]);
  });

  it("highlights nothing when only domains are given", () => {
    const r = resolveErdFocus({ domains: ["sales"], objects: [] }, TABLES, DOMAINS);
    expect(r.shownDomains).toEqual(new Set(["sales"]));
    expect(r.highlighted).toEqual([]);
  });

  it("shows the domains given, whatever the objects' own are", () => {
    const r = resolveErdFocus({ domains: ["sales"], objects: ["finance.ledger"] }, TABLES, DOMAINS);
    expect(r.shownDomains).toEqual(new Set(["sales"]));
    // Named, and in no domain shown: nothing is drawn to highlight.
    expect(r.highlighted).toEqual([]);
    expect(r.tables).toEqual(["finance.ledger"]);
    expect(r.missing).toEqual([]);
  });

  it("names what does not exist and keeps what does", () => {
    const r = resolveErdFocus(
      { domains: ["sales", "atlantis"], objects: ["sales.orders", "sales.ghosts"] },
      TABLES,
      DOMAINS,
    );
    expect(r.shownDomains).toEqual(new Set(["sales"]));
    expect(r.highlighted).toEqual([1]);
    expect(r.domains).toEqual(["sales"]);
    expect(r.tables).toEqual(["sales.orders"]);
    expect(r.missing).toEqual(["atlantis", "sales.ghosts"]);
  });

  it("takes a domain with no tables as existing, and a table's domain as existing", () => {
    expect(resolveErdFocus({ domains: ["empty"], objects: [] }, TABLES, DOMAINS).missing).toEqual(
      [],
    );
    expect(resolveErdFocus({ domains: ["finance"], objects: [] }, TABLES, []).missing).toEqual([]);
  });

  it("shows nothing when nothing named exists", () => {
    const r = resolveErdFocus({ domains: [], objects: ["sales.ghosts"] }, TABLES, DOMAINS);
    expect(r.shownDomains).toEqual(new Set());
    expect(r.missing).toEqual(["sales.ghosts"]);
  });
});

describe("the panel's actions", () => {
  it("Focus is the table's domain and the table, whatever was focused before", () => {
    expect(focusOnTable(TABLES[2])).toEqual({ domains: ["finance"], objects: ["finance.ledger"] });
  });

  it("Add to focus adds the table and a new domain, once each", () => {
    const focus = { domains: ["sales"], objects: ["sales.orders"] };
    expect(addTableToFocus(focus, TABLES[1])).toEqual({
      domains: ["sales"],
      objects: ["sales.orders", "sales.customers"],
    });
    expect(addTableToFocus(focus, TABLES[2])).toEqual({
      domains: ["sales", "finance"],
      objects: ["sales.orders", "finance.ledger"],
    });
    expect(addTableToFocus(focus, TABLES[0])).toEqual(focus);
  });

  it("leaves a link of objects alone naming no domains: its objects' domains are the ones shown", () => {
    expect(addTableToFocus({ domains: [], objects: ["sales.orders"] }, TABLES[2])).toEqual({
      domains: [],
      objects: ["sales.orders", "finance.ledger"],
    });
  });
});

describe("highlightErdTables", () => {
  it("marks the tables drawn, fits them into view, and says which", () => {
    const f = fakeCy(["t:1", "t:2"]);
    expect(highlightErdTables(f.cy, [1, 2])).toEqual([1, 2]);
    expect([...f.classed]).toEqual(["t:1", "t:2"]);
    expect(f.fit).toHaveBeenCalledOnce();
  });

  it("reports a table the diagram does not hold and takes no other", () => {
    const f = fakeCy(["t:7"]);
    expect(highlightErdTables(f.cy, [99])).toEqual([]);
    expect(f.classed.size).toBe(0);
    expect(f.fit).not.toHaveBeenCalled();
  });

  it("does not blow one table up to the whole canvas", () => {
    const f = fakeCy(["t:7"], 4);
    highlightErdTables(f.cy, [7]);
    expect(f.zoom).toHaveBeenCalledWith(1.5);
    expect(f.center).toHaveBeenCalledOnce();
  });
});
