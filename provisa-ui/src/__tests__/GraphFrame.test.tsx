// Copyright (c) 2026 Kenneth Stott
// Canary: 2e9f1b4c-7a3d-4e8b-9c5f-1d2a6b7e3f8c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * Unit tests for GraphFrame.tsx pure utility functions:
 * PALETTE, labelColor, isNode, isEdge, extractElements
 */

import { describe, it, expect } from "vitest";
import {
  PALETTE,
  labelColor,
  isNode,
  isEdge,
  extractElements,
} from "../pages/GraphFrame";

// ── labelColor ────────────────────────────────────────────────────────────────

describe("labelColor", () => {
  it("returns a hex string from PALETTE", () => {
    const color = labelColor("Person");
    expect(PALETTE).toContain(color);
  });

  it("returns the same color for the same label", () => {
    expect(labelColor("Order")).toBe(labelColor("Order"));
  });

  it("different labels may produce different colors", () => {
    // Hash collision is possible but unlikely for short distinct strings
    const colors = new Set(["Person", "Order", "Product", "Company", "Event"].map(labelColor));
    expect(colors.size).toBeGreaterThan(1);
  });

  it("handles empty string without throwing", () => {
    expect(() => labelColor("")).not.toThrow();
    expect(PALETTE).toContain(labelColor(""));
  });
});

// ── isNode ────────────────────────────────────────────────────────────────────

describe("isNode", () => {
  it("returns true for a valid GNode", () => {
    expect(isNode({ id: "1", label: "Person", properties: { name: "Alice" } })).toBe(true);
  });

  it("returns false when label is missing", () => {
    expect(isNode({ id: "1", properties: {} })).toBe(false);
  });

  it("returns false when properties is missing", () => {
    expect(isNode({ id: "1", label: "Person" })).toBe(false);
  });

  it("returns false when startNode is present (edge-like)", () => {
    expect(
      isNode({ id: "1", label: "X", properties: {}, startNode: {}, endNode: {} }),
    ).toBe(false);
  });

  it("returns false for primitives", () => {
    expect(isNode(null)).toBe(false);
    expect(isNode(undefined)).toBe(false);
    expect(isNode(42)).toBe(false);
    expect(isNode("string")).toBe(false);
  });
});

// ── isEdge ────────────────────────────────────────────────────────────────────

describe("isEdge", () => {
  const startNode = { id: "1", label: "Person", properties: {} };
  const endNode = { id: "2", label: "Order", properties: {} };

  it("returns true for a valid GEdge", () => {
    expect(
      isEdge({ id: "e1", type: "PLACED", startNode, endNode, properties: {} }),
    ).toBe(true);
  });

  it("returns false when type is missing", () => {
    expect(isEdge({ id: "e1", startNode, endNode, properties: {} })).toBe(false);
  });

  it("returns false when startNode is missing", () => {
    expect(isEdge({ id: "e1", type: "PLACED", endNode, properties: {} })).toBe(false);
  });

  it("returns false when endNode is missing", () => {
    expect(isEdge({ id: "e1", type: "PLACED", startNode, properties: {} })).toBe(false);
  });

  it("returns false for primitives", () => {
    expect(isEdge(null)).toBe(false);
    expect(isEdge(42)).toBe(false);
  });
});

// ── extractElements ────────────────────────────────────────────────────────────

describe("extractElements", () => {
  const alice: Record<string, unknown> = { id: "n1", label: "Person", properties: { name: "Alice" } };
  const acme: Record<string, unknown> = { id: "n2", label: "Company", properties: { name: "Acme" } };
  const edge: Record<string, unknown> = {
    id: "e1",
    type: "WORKS_AT",
    startNode: alice,
    endNode: acme,
    properties: {},
  };

  it("extracts a node from a top-level row value", () => {
    const { nodes } = extractElements([{ n: alice }]);
    expect(nodes.has("n1")).toBe(true);
    expect(nodes.get("n1")?.label).toBe("Person");
  });

  it("extracts an edge and its implied nodes", () => {
    const { nodes, edges } = extractElements([{ r: edge }]);
    expect(edges.has("e1")).toBe(true);
    expect(nodes.has("n1")).toBe(true);
    expect(nodes.has("n2")).toBe(true);
  });

  it("deduplicates nodes that appear multiple times", () => {
    const rows = [{ n: alice }, { n: alice }, { r: edge }];
    const { nodes } = extractElements(rows);
    expect(nodes.size).toBe(2); // n1 and n2 only
  });

  it("handles nested arrays in rows", () => {
    const { nodes } = extractElements([[alice, acme]]);
    expect(nodes.size).toBe(2);
  });

  it("returns empty maps for empty input", () => {
    const { nodes, edges } = extractElements([]);
    expect(nodes.size).toBe(0);
    expect(edges.size).toBe(0);
  });

  it("ignores null and undefined row values", () => {
    const { nodes, edges } = extractElements([{ a: null }, { b: undefined }]);
    expect(nodes.size).toBe(0);
    expect(edges.size).toBe(0);
  });

  it("ignores scalar row values", () => {
    const { nodes, edges } = extractElements([{ count: 42, name: "hello" }]);
    expect(nodes.size).toBe(0);
    expect(edges.size).toBe(0);
  });

  it("preserves edge properties", () => {
    const edgeWithProps: Record<string, unknown> = {
      id: "e2", type: "KNOWS",
      startNode: alice, endNode: acme,
      properties: { since: 2020 },
    };
    const { edges } = extractElements([{ r: edgeWithProps }]);
    expect(edges.get("e2")?.properties).toEqual({ since: 2020 });
  });
});
