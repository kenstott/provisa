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
import { useScreenEditor, useScreenGrid, type ScreenEditor } from "../mcp/screenState";

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

beforeEach(() => {
  // jsdom lays nothing out; every element counts as drawn unless a test says otherwise.
  HTMLElement.prototype.checkVisibility = function () {
    return this.dataset.offscreen !== "true";
  };
  window.history.pushState({}, "", "/sql?tab=1");
});

afterEach(() => cleanup());

describe("read_screen", () => {
  it("reports the route and the editor of the route on screen, with its tab", async () => {
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
    window.history.pushState({}, "", "/sources");
    render(<Editor route="/sql" editor={{ language: "sql", text: "SELECT 1", tab: "Q" }} />);
    expect((await screen()).editor).toBeNull();
  });

  it("drops an editor once it unmounts", async () => {
    const { unmount } = render(
      <Editor route="/sql" editor={{ language: "sql", text: "SELECT 1", tab: "Q" }} />,
    );
    unmount();
    expect((await screen()).editor).toBeNull();
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

  it("reports a result grid's columns and row count, not its rows", async () => {
    render(<Grid />);
    expect((await screen()).results).toEqual([
      { columns: ["name", "species"], rows: 3, partial: false },
    ]);
  });

  it("cuts its answer at the size admin_request's answers are cut at", async () => {
    render(
      <Editor route="/sql" editor={{ language: "sql", text: "x".repeat(10_000), tab: "Q" }} />,
    );
    const result = await executeClientTool("read_screen", {}, ctx);
    expect(result.message).toHaveLength(4000);
  });
});
