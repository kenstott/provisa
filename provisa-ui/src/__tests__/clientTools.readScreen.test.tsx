// Copyright (c) 2026 Kenneth Stott
// Canary: bb03e25a-b17e-48c4-b494-98b3909d03d3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2115: Polly's read_screen reports the route, the open editor from its own state, the visible
// labelled fields (a password as dots) and each result grid's columns and row count, never rows.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { executeClientTool, type ClientToolContext } from "../mcp/clientTools";
import {
  chooseDataAddress,
  DATA_ADDRESS_KEY,
  requestDataAddresses,
  resetDataAddresses,
} from "../lib/dataAddress";
import { removeStored } from "../lib/browserStorage";
import { readScreen, useScreenEditor, useScreenGrid, type ScreenEditor } from "../mcp/screenState";

const ctx: ClientToolContext = {
  navigate: vi.fn(),
  confirm: vi.fn(),
  runMutation: vi.fn(),
  presentChoice: vi.fn(),
  runAdminMutation: vi.fn(),
  chooseFile: vi.fn(),
};

function Editor({ route, editor }: { route: string; editor: ScreenEditor }) {
  useScreenEditor(route, editor);
  return null;
}

function Grid() {
  useScreenGrid({ columns: ["name", "species"], rows: 3, partial: false });
  return null;
}

async function screen() {
  const result = await executeClientTool("read_screen", {}, ctx);
  expect(result.success).toBe(true);
  return JSON.parse(result.message);
}

const originalFetch = window.fetch;

/** The coordinator's answer to GET /admin/data-addresses, once the session has asked for it. */
function offerFromCoordinator(offer: unknown, status = 200) {
  window.fetch = vi.fn(
    async () => new Response(JSON.stringify(offer), { status }),
  ) as typeof window.fetch;
  requestDataAddresses();
}

beforeEach(() => {
  resetDataAddresses();
  removeStored(DATA_ADDRESS_KEY);
  // jsdom lays nothing out; every element counts as drawn unless a test says otherwise.
  HTMLElement.prototype.checkVisibility = function () {
    return this.dataset.offscreen !== "true";
  };
  // A page with no query editor; a test of one opens its route.
  window.history.pushState({}, "", "/sources");
});

afterEach(() => {
  cleanup();
  window.fetch = originalFetch;
});

describe("read_screen", () => {
  it("reports the route and the editor of the route on screen, with its tab", async () => {
    window.history.pushState({}, "", "/sql?tab=1");
    render(
      <>
        <Editor
          route="/sql"
          editor={{ language: "sql", text: "SELECT * FROM pets", tab: "Query 2" }}
        />
        <Editor route="/python" editor={{ language: "python", text: "print(1)", tab: "S1" }} />
      </>,
    );
    const s = await screen();
    expect(s.route).toBe("/sql?tab=1");
    expect(s.editor).toEqual({ language: "sql", text: "SELECT * FROM pets", tab: "Query 2" });
  });

  it("reports no editor on a route that shows none", async () => {
    render(<Editor route="/sql" editor={{ language: "sql", text: "SELECT 1", tab: "Q" }} />);
    expect((await screen()).editor).toBeNull();
  });

  it("drops an editor once it unmounts: its route then reads as not loaded", async () => {
    window.history.pushState({}, "", "/sql");
    const { unmount } = render(
      <Editor route="/sql" editor={{ language: "sql", text: "SELECT 1", tab: "Q" }} />,
    );
    unmount();
    expect(await readScreen(50)).toBe("the editor on /sql has not loaded within 0.05s");
  });

  it("waits for the editor of a route still loading, and reads it once it publishes", async () => {
    window.history.pushState({}, "", "/python");
    const reading = executeClientTool("read_screen", {}, ctx);
    render(<Editor route="/python" editor={{ language: "python", text: "print(1)", tab: "S1" }} />);
    const result = await reading;
    expect(result.success).toBe(true);
    expect(JSON.parse(result.message).editor).toEqual({
      language: "python",
      text: "print(1)",
      tab: "S1",
    });
  });

  it("says so when the route's editor never loads, rather than reading no editor", async () => {
    window.history.pushState({}, "", "/graph");
    expect(await readScreen(50)).toBe("the editor on /graph has not loaded within 0.05s");
  });

  it("reports the visible labelled fields, a password as dots, and not Polly's own input", async () => {
    render(
      <>
        <label htmlFor="host">Host</label>
        <input id="host" defaultValue="db.local" />
        <label htmlFor="pw">Password</label>
        <input id="pw" type="password" defaultValue="hunter2" />
        <input aria-label="Read only" type="checkbox" defaultChecked />
        <input aria-label="Elsewhere" defaultValue="x" data-offscreen="true" />
        <div hidden>
          <input aria-label="Hidden" defaultValue="y" />
        </div>
        <input defaultValue="no label" />
        <div data-testid="chat-panel">
          <input aria-label="Ask Polly" defaultValue="put this into python" />
        </div>
      </>,
    );
    expect((await screen()).fields).toEqual([
      { label: "Host", value: "db.local" },
      { label: "Password", value: "•••••••" },
      { label: "Read only", value: true },
    ]);
  });

  it("reports no data node when the UI is not coordinator-served", async () => {
    expect((await screen()).dataNode).toBeNull();
    offerFromCoordinator({ choose: false, addresses: [] });
    expect((await screen()).dataNode).toBeNull();
  });

  it("reports the data node a coordinator-served UI uses: the first offered until one is chosen", async () => {
    offerFromCoordinator({
      choose: true,
      addresses: ["https://q1.example.com", "https://q2.example.com"],
    });
    expect((await screen()).dataNode).toBe("https://q1.example.com");
  });

  it("reports the chosen data node, on any route", async () => {
    offerFromCoordinator({
      choose: true,
      addresses: ["https://q1.example.com", "https://q2.example.com"],
    });
    chooseDataAddress("https://typed.example.com:8443");
    window.history.pushState({}, "", "/admin/billing");
    expect((await screen()).dataNode).toBe("https://typed.example.com:8443");
  });

  it("says so when the data node cannot be read, rather than reporting none", async () => {
    offerFromCoordinator({}, 503);
    const result = await executeClientTool("read_screen", {}, ctx);
    expect(result.success).toBe(false);
    expect(result.message).toContain("GET /admin/data-addresses 503");
  });

  it("reports a result grid's columns and row count, not its rows", async () => {
    render(<Grid />);
    expect((await screen()).results).toEqual([
      { columns: ["name", "species"], rows: 3, partial: false },
    ]);
  });

  it("cuts its answer at the size admin_request's answers are cut at", async () => {
    window.history.pushState({}, "", "/sql");
    render(
      <Editor route="/sql" editor={{ language: "sql", text: "x".repeat(10_000), tab: "Q" }} />,
    );
    const result = await executeClientTool("read_screen", {}, ctx);
    expect(result.message).toHaveLength(4000);
  });
});
