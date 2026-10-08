// Copyright (c) 2026 Kenneth Stott
// Canary: 4f8a1d3c-7b26-4e59-9a0d-6c2e8b5f1d47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: a branded source is picked and shown by its brand, and registered and classified
// as the generic source that carries it.

import { describe, expect, it } from "vitest";
import { SOURCE_TYPES } from "../pages/sources/constants";
import {
  backendType,
  carrierOf,
  reachInfoFor,
  sourceBrand,
  sourceTypeLabel,
  uiType,
} from "../pages/sources/sourceHelpers";
import type { FederationEngineState } from "../api/admin";

describe("branded source types", () => {
  it("offers GitHub and GitLab in the source picker", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "github")?.label).toBe("GitHub");
    expect(SOURCE_TYPES.find((s) => s.value === "gitlab")?.label).toBe("GitLab");
  });

  it("classifies a brand as the backend type that carries it", () => {
    expect(backendType("github")).toBe("graphql_remote");
    expect(backendType("graphql")).toBe("graphql_remote");
    expect(backendType("postgresql")).toBe("postgresql");
  });

  it("offers Stripe, carried by the OpenAPI source", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "stripe")?.label).toBe("Stripe");
    expect(backendType("stripe")).toBe("openapi");
    expect(carrierOf("stripe")).toBe("openapi");
    expect(carrierOf("github")).toBe("graphql");
    expect(carrierOf("openapi")).toBe("openapi");
    expect(sourceTypeLabel("openapi", '{"brand":"stripe"}')).toBe("Stripe");
    expect(sourceTypeLabel("openapi", "{}")).toBe("REST API (OpenAPI)");
  });

  it("still maps the plain remote GraphQL backend type to its own picker entry", () => {
    expect(uiType("graphql_remote")).toBe("graphql");
  });

  it("reads the brand a source row records", () => {
    expect(sourceBrand('{"brand":"github","namespace":"gh"}')).toBe("github");
    expect(sourceBrand('{"namespace":"shop"}')).toBeNull();
    expect(sourceBrand(null)).toBeNull();
    expect(sourceBrand('{"brand":"not-carried"}')).toBeNull();
  });

  it("shows a branded source by its brand and a plain one by its type", () => {
    expect(sourceTypeLabel("graphql_remote", '{"brand":"github"}')).toBe("GitHub");
    expect(sourceTypeLabel("graphql_remote", '{"brand":"gitlab"}')).toBe("GitLab");
    expect(backendType("gitlab")).toBe("graphql_remote");
    expect(sourceTypeLabel("graphql_remote", "{}")).toBe("GraphQL");
    expect(sourceTypeLabel("postgresql", null)).toBe("PostgreSQL");
  });

  it("is selectable wherever the remote GraphQL source is", () => {
    const engineState = {
      current: "duckdb",
      engines: [
        {
          key: "duckdb",
          label: "DuckDB",
          live_source_types: [],
          reachable_source_types: ["graphql_remote"],
        },
      ],
    } as unknown as FederationEngineState;
    expect(reachInfoFor("github", engineState)).toEqual(reachInfoFor("graphql", engineState));
    expect(reachInfoFor("github", engineState).selectable).toBe(true);
  });
});
