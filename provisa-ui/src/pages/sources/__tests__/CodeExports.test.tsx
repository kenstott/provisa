// Copyright (c) 2026 Kenneth Stott
// Canary: b594f052-a84a-446f-aa24-3ab0cbdb118c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: a saved code source's form lists what its code exports, each function's name and
// kind, read only, in the read view and the edit form alike.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "../../../test-utils/render";
import { FormLayout, FormRead } from "../../../components/form/FormLayout";
import { CodeExports } from "../CodeExports";

const EXPORTS = [
  { name: "scores", kind: "table" },
  { name: "retrain", kind: "command" },
  { name: "ticks", kind: "stream" },
];
let asked: string[] = [];

beforeEach(() => {
  asked = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      asked.push(String(url));
      return new Response(JSON.stringify({ exports: EXPORTS }), { status: 200 });
    }),
  );
});
afterEach(() => vi.unstubAllGlobals());

const lines = () => [...screen.getByTestId("code-exports-list").children].map((l) => l.textContent);

describe("a code source's exports", () => {
  it("are listed with their kinds in the edit form", async () => {
    render(
      <FormLayout>
        <CodeExports orgId="acme" sourceId="churn-model" />
      </FormLayout>,
    );
    await waitFor(() =>
      expect(lines()).toEqual(["scores · table", "retrain · command", "ticks · stream"]),
    );
    expect(asked).toEqual(["/admin/orgs/acme/code-sources/churn-model/exports"]);
  });

  it("read as the same list in the read view", async () => {
    render(
      <FormRead testId="read">
        <CodeExports orgId="acme" sourceId="churn-model" />
      </FormRead>,
    );
    await waitFor(() =>
      expect(lines()).toEqual(["scores · table", "retrain · command", "ticks · stream"]),
    );
    const row = [...screen.getByTestId("read").querySelectorAll("dt")].map((d) => d.textContent);
    expect(row).toEqual(["Exports"]);
  });

  it("say why when the code cannot be read", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify({ detail: "no uv.lock" }), { status: 400 })),
    );
    render(
      <FormLayout>
        <CodeExports orgId="acme" sourceId="churn-model" />
      </FormLayout>,
    );
    await waitFor(() =>
      expect(screen.getByTestId("code-exports").textContent).toContain("no uv.lock"),
    );
  });
});
