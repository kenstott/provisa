// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The GraphQL explorer keeps its last response with the exact query that produced it, and opened
// again restores it only beside that query. The maintainer's case: glossary_terms' answer was
// restored beside ps__petCompanions, a query edited in after the run.

import { beforeEach, describe, expect, it, vi } from "vitest";

const idbStore = new Map<string, unknown>();
vi.mock("idb-keyval", () => ({
  get: vi.fn(async (k: string) => idbStore.get(k)),
  set: vi.fn(async (k: string, v: unknown) => void idbStore.set(k, v)),
}));

import { keepResponse, noteRun, restoreResponse } from "../graphqlResponseStore";

const GLOSSARY = "{ glossary_terms { name } }";
const PET_COMPANIONS = "{ ps__petCompanions }";

beforeEach(() => idbStore.clear());

describe("the GraphQL explorer's kept response", () => {
  it("is restored beside the query that produced it", async () => {
    noteRun(GLOSSARY);
    await keepResponse(GLOSSARY, '{"data":{"glossary_terms":[]}}');
    expect(await restoreResponse(GLOSSARY)).toBe('{"data":{"glossary_terms":[]}}');
  });

  it("is not restored beside a query edited in after the run", async () => {
    noteRun(GLOSSARY);
    await keepResponse(GLOSSARY, '{"data":{"glossary_terms":[]}}');
    expect(await restoreResponse(PET_COMPANIONS)).toBeNull();
  });

  it("is a failed run's error once that run answers, never the run before's result", async () => {
    noteRun(GLOSSARY);
    await keepResponse(GLOSSARY, '{"data":{"glossary_terms":[]}}');
    noteRun(PET_COMPANIONS);
    await keepResponse(PET_COMPANIONS, '{"errors":[{"message":"must have a selection"}]}');
    expect(await restoreResponse(PET_COMPANIONS)).toBe(
      '{"errors":[{"message":"must have a selection"}]}',
    );
  });

  it("is not kept beside a query edited while the run was out", async () => {
    noteRun(GLOSSARY);
    await keepResponse(GLOSSARY + " # edited", '{"data":{"glossary_terms":[]}}');
    expect(await restoreResponse(GLOSSARY + " # edited")).toBeNull();
  });

  it("stays paired with its query once restored, so showing it keeps it", async () => {
    noteRun(GLOSSARY);
    await keepResponse(GLOSSARY, "answer");
    // Another tab's run since: the restore pairs the shown response with its own query again.
    noteRun(PET_COMPANIONS);
    expect(await restoreResponse(GLOSSARY)).toBe("answer");
    await keepResponse(GLOSSARY, "answer");
    expect(await restoreResponse(GLOSSARY)).toBe("answer");
  });
});
