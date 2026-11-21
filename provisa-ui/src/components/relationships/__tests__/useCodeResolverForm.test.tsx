// Copyright (c) 2026 Kenneth Stott
// Canary: 0d9e7a52-3b64-4f18-a6c1-e2f4b8d07a93
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2249: a code resolver set is saved with its incremental setting and the interval between
// whole rebuilds that requires; a save refused for either is said in the catalog's words.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";
import type { CodeResolver, MutationResult } from "../../../types/admin";
import { i18nReady } from "../../../i18n";
import { EMPTY_FORM, type RelForm } from "../relationship-types";

const saveCodeResolver = vi.fn<(input: unknown, isNew: boolean) => Promise<MutationResult>>();
const inService: CodeResolver[] = [];

vi.mock("../../../hooks/useAdminQueries", () => ({
  useCodeResolvers: () => ({
    codeResolvers: inService,
    loading: false,
    saveCodeResolver,
    retireCodeResolver: vi.fn(),
  }),
}));

import { useCodeResolverForm } from "../useCodeResolverForm";

const filled: RelForm = {
  ...EMPTY_FORM,
  id: "store_regions",
  targetType: "resolver",
  sourceTableId: "stores",
  sourceColumn: "id",
  targetTableId: "regions",
  targetColumn: "id",
  resolver: "match_stores",
  sourceMatch: "store_id",
  targetMatch: "region_id",
};

function page(form: RelForm) {
  return {
    form,
    setForm: vi.fn(),
    setShowForm: vi.fn(),
    setSaveProblem: vi.fn(),
    detail: { open: vi.fn(), close: vi.fn() },
    refusal: { refused: vi.fn(() => false) },
  };
}

/** What the form last said of its save. */
const said = (p: ReturnType<typeof page>) => p.setSaveProblem.mock.calls.at(-1)?.[0] as string;

beforeEach(async () => {
  await i18nReady;
  saveCodeResolver.mockReset();
  inService.length = 0;
});

describe("saving a code resolver set's incremental setting", () => {
  it("saves it off, with no interval, when it is not chosen", async () => {
    saveCodeResolver.mockResolvedValue({ success: true, message: "" });
    const p = page(filled);
    const { result } = renderHook(() => useCodeResolverForm(p));
    await act(() => result.current.save());
    expect(saveCodeResolver).toHaveBeenCalledWith(
      expect.objectContaining({ id: "store_regions", incremental: false, rebuildEvery: null }),
      true,
    );
  });

  it("saves it on with the interval between whole rebuilds", async () => {
    saveCodeResolver.mockResolvedValue({ success: true, message: "" });
    const p = page({ ...filled, incremental: true, rebuildEvery: "3600" });
    const { result } = renderHook(() => useCodeResolverForm(p));
    await act(() => result.current.save());
    expect(saveCodeResolver).toHaveBeenCalledWith(
      expect.objectContaining({ incremental: true, rebuildEvery: 3600 }),
      true,
    );
  });

  it("asks for the interval before saving it on, in the catalog's words", async () => {
    const p = page({ ...filled, incremental: true, rebuildEvery: "" });
    const { result } = renderHook(() => useCodeResolverForm(p));
    await act(() => result.current.save());
    expect(saveCodeResolver).not.toHaveBeenCalled();
    expect(said(p)).toContain("store_regions");
    expect(said(p)).toContain("Whole rebuild every");
    expect(p.setShowForm).not.toHaveBeenCalled();
  });

  for (const [code, words] of [
    ["schema.incremental_no_change", "has no @change function for store_regions"],
    ["schema.incremental_needs_rebuild_every", "Whole rebuild every"],
  ] as const) {
    it(`says a save refused with ${code} in the catalog's words`, async () => {
      saveCodeResolver.mockResolvedValue({
        success: false,
        code,
        params: { table: "store_regions" },
        message: "server english text",
      });
      const p = page({ ...filled, incremental: true, rebuildEvery: "3600" });
      const { result } = renderHook(() => useCodeResolverForm(p));
      await act(() => result.current.save());
      expect(said(p)).toContain(words);
      expect(said(p)).not.toContain("server english text");
      // The form stays open on what was refused.
      expect(p.setShowForm).not.toHaveBeenCalled();
    });
  }

  it("opens a set in service with its setting and interval", () => {
    inService.push({
      id: "store_regions",
      resolver: "match_stores",
      sourceTable: "stores",
      sourceColumn: "id",
      sourceMatch: "store_id",
      targetTable: "regions",
      targetColumn: "id",
      targetMatch: "region_id",
      refreshInterval: 300,
      incremental: true,
      rebuildEvery: 3600,
      incrementalOption: { available: true, code: null, reason: null },
    });
    const p = page(EMPTY_FORM);
    const { result } = renderHook(() => useCodeResolverForm(p));
    act(() => result.current.change("store_regions"));
    expect(p.setForm).toHaveBeenCalledWith(
      expect.objectContaining({ incremental: true, rebuildEvery: "3600" }),
    );
  });
});
