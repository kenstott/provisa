// Copyright (c) 2026 Kenneth Stott
// Canary: 6398a074-1f1a-40ba-a764-dd1b58cec262
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// /data/sql answers a statement in one of two shapes: its rows with their columns, or a handle to
// the results store when the result was landed there (`{"data":{"sql":null},"redirect":{…}}`).
// runSql says which it is; it never hands a caller rows and columns it did not get.

import i18n from "i18next";
import { afterEach, describe, expect, it, vi } from "vitest";
import { explainSql, nlToSql, runSql } from "../admin";
import en from "../../i18n/locales/en/serverErrors.json";

function answer(body: unknown) {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    ),
  );
}

afterEach(() => vi.unstubAllGlobals());

describe("runSql — what /data/sql answered", () => {
  it("returns the rows and their columns", async () => {
    answer({ data: { sql: [{ id: 1 }] }, columns: ["id"], column_types: ["integer"] });
    const r = await runSql("SELECT 1");
    expect(r.error).toBeUndefined();
    expect(r.redirect).toBeUndefined();
    expect(r.columns).toEqual(["id"]);
    expect(r.rows).toEqual([{ id: 1 }]);
  });

  it("says the result was landed in the results store, with where and how many rows", async () => {
    answer({
      data: { sql: null },
      redirect: {
        redirect_url: "https://results.example/r/abc.parquet",
        row_count: 2500000,
        expires_in: 3600,
        content_type: "application/vnd.apache.parquet",
      },
    });
    const r = await runSql("SELECT * FROM big");
    expect(r.error).toBeUndefined();
    expect(r.redirect).toEqual({
      url: "https://results.example/r/abc.parquet",
      rowCount: 2500000,
      expiresIn: 3600,
      contentType: "application/vnd.apache.parquet",
    });
    expect(r.rows).toEqual([]);
    expect(r.columns).toEqual([]);
  });

  it("fails loud when the answer carries no column list, never handing on an undefined one", async () => {
    answer({ data: { sql: [] }, detail: "the engine returned no description" });
    const r = await runSql("SELECT 1");
    expect(r.columns).toEqual([]);
    expect(r.error).toContain("the engine returned no description");
  });
});

describe("runSql — a statement the server refused", () => {
  function refuse(status: number, body: string) {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(body, { status })));
  }

  it("says a coded refusal in the reader's words, with its params, not the body it came in", async () => {
    // The provisioning close, read outside its window: the explorer showed the raw JSON.
    await i18n.init({ lng: "en", resources: { en: { translation: en } } });
    refuse(
      503,
      JSON.stringify({
        detail: "the view 'view_close_by_category' can be read only within its read window",
        code: "data.view_outside_load_window",
        params: { view: "view_close_by_category", opens_at: "2026-10-22T02:00+00:00" },
      }),
    );
    const r = await runSql("SELECT 1");
    expect(r.error).toBe(
      "The view view_close_by_category can be read only within its read window. It next opens at 2026-10-22T02:00+00:00.",
    );
  });

  it("says an uncoded refusal in the server's own words", async () => {
    refuse(400, JSON.stringify({ detail: "syntax error at or near FROM" }));
    expect((await runSql("SELECT FROM")).error).toBe("syntax error at or near FROM");
  });

  it("refuses an explain in the same words, as the analyze and detach blocks show it", async () => {
    await i18n.init({ lng: "en", resources: { en: { translation: en } } });
    refuse(
      503,
      JSON.stringify({
        detail: "raw",
        code: "data.view_outside_load_window",
        params: { view: "v", opens_at: "02:00" },
      }),
    );
    await expect(explainSql("SELECT 1")).rejects.toThrow(
      "The view v can be read only within its read window. It next opens at 02:00.",
    );
  });

  it("says an NL question's refusal in the same words", async () => {
    await i18n.init({ lng: "en", resources: { en: { translation: en } } });
    refuse(
      503,
      JSON.stringify({
        detail: "raw",
        code: "data.view_outside_load_window",
        params: { view: "v", opens_at: "02:00" },
      }),
    );
    expect((await nlToSql("what closed?")).error).toBe(
      "The view v can be read only within its read window. It next opens at 02:00.",
    );
  });

  it("says a body that is not JSON as it is", async () => {
    refuse(502, "Bad Gateway");
    expect((await runSql("SELECT 1")).error).toBe("Bad Gateway");
  });
});
