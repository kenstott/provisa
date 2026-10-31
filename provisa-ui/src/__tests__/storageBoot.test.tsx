// Copyright (c) 2026 Kenneth Stott
// Canary: a93b6e24-0d17-4fb8-9c61-7e2d4c8f1a35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// As the final backstop, no storage failure can fail the UI's start: main.tsx itself is run against
// a store that is full, one that throws on every call, and a sandbox hand-off neither will keep.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { fullStorage, installStorage, throwingStorage } from "./mockStorage";

let undo: (() => void)[] = [];
let errors: ReturnType<typeof vi.spyOn>;

beforeEach(() => {
  vi.resetModules();
  document.body.innerHTML = '<div id="root"></div>';
  // A control plane answering a caller with no session.
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(JSON.stringify({ detail: "no session" }), { status: 401 })),
  );
  errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
});

afterEach(() => {
  undo.forEach((u) => u());
  undo = [];
  vi.unstubAllGlobals();
  errors.mockRestore();
  window.location.hash = "";
});

async function boot(): Promise<HTMLElement> {
  await import("../main.tsx");
  const root = document.getElementById("root")!;
  await waitFor(() => expect(root.childElementCount).toBeGreaterThan(0), { timeout: 30000 });
  expect(root.textContent).not.toContain("failed to start");
  return root;
}

describe("the UI starts", () => {
  it("with storage that throws on every call, and says so once", async () => {
    undo.push(installStorage("localStorage", throwingStorage()));
    undo.push(installStorage("sessionStorage", throwingStorage()));
    await boot();
    expect(await screen.findAllByText(/could not save some settings/)).toHaveLength(1);
    expect(errors).toHaveBeenCalled();
  }, 60000);

  it("with storage that is full", async () => {
    undo.push(
      installStorage(
        "localStorage",
        fullStorage(60, { "cp:org:provisa.graph.state": "x".repeat(40) }),
      ),
    );
    undo.push(installStorage("sessionStorage", fullStorage(0)));
    await boot();
  }, 60000);

  it("with corrupt values under the keys it reads", async () => {
    localStorage.setItem("provisa_tour_topics_done", "{not json");
    localStorage.setItem("provisa-color-scheme", "{not json");
    await boot();
    localStorage.clear();
  }, 60000);

  it("signed out, with the failure noticed, when a sandbox token cannot be kept", async () => {
    undo.push(installStorage("localStorage", fullStorage(0)));
    undo.push(installStorage("sessionStorage", fullStorage(0)));
    window.location.hash = "#provisa_sandbox=provisa_pat_visitor&provisa_sandbox_org=sbx";
    await boot();
    expect(await screen.findByText(/would not keep your session/)).toBeInTheDocument();
    expect(window.location.hash).toBe(""); // the token never stays on the address
    const { visitorActive } = await import("../lib/browserStorage");
    expect(visitorActive()).toBe(false);
  }, 60000);
});
