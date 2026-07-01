// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { describe, it, expect } from "vitest";
import { liveCapability, cdcTransportApplicable } from "../liveCapability";

describe("liveCapability", () => {
  it("postgresql supports both poll and cdc", () => {
    const c = liveCapability("postgresql");
    expect(c).toEqual({ pollAvail: true, cdcAvail: true, liveCapable: true });
  });

  it("mongodb supports both poll and cdc", () => {
    expect(liveCapability("mongodb")).toEqual({
      pollAvail: true,
      cdcAvail: true,
      liveCapable: true,
    });
  });

  it("debezium is cdc-only (push feed, not pollable)", () => {
    expect(liveCapability("debezium")).toEqual({
      pollAvail: false,
      cdcAvail: true,
      liveCapable: true,
    });
  });

  it("kafka is cdc-only (push feed, not pollable)", () => {
    expect(liveCapability("kafka")).toEqual({
      pollAvail: false,
      cdcAvail: true,
      liveCapable: true,
    });
  });

  it("snowflake is poll-only (federated SQL, no push provider)", () => {
    expect(liveCapability("snowflake")).toEqual({
      pollAvail: true,
      cdcAvail: false,
      liveCapable: true,
    });
  });

  it("mysql/oracle are poll-only — cdc greyed out", () => {
    for (const t of ["mysql", "oracle", "mariadb", "sqlserver"]) {
      const c = liveCapability(t);
      expect(c.pollAvail).toBe(true);
      expect(c.cdcAvail).toBe(false);
    }
  });

  it("api/file/graph sources are not live-capable", () => {
    for (const t of ["openapi", "graphql", "grpc", "csv", "sqlite", "neo4j", "sparql"]) {
      expect(liveCapability(t).liveCapable).toBe(false);
    }
  });

  it("is case-insensitive and null-safe", () => {
    expect(liveCapability("PostgreSQL").liveCapable).toBe(true);
    expect(liveCapability(null).liveCapable).toBe(false);
    expect(liveCapability(undefined).liveCapable).toBe(false);
    expect(liveCapability("").liveCapable).toBe(false);
  });
});

describe("cdcTransportApplicable (REQ-824)", () => {
  it("applies to non-PG RDBMS reached via Debezium", () => {
    for (const t of ["mysql", "mariadb", "sqlserver", "oracle"]) {
      expect(cdcTransportApplicable(t)).toBe(true);
    }
  });

  it("does not apply to postgres (native LISTEN/NOTIFY needs no transport)", () => {
    expect(cdcTransportApplicable("postgresql")).toBe(false);
  });

  it("does not apply to non-RDBMS / poll-only sources", () => {
    for (const t of ["kafka", "mongodb", "snowflake", "csv", "neo4j"]) {
      expect(cdcTransportApplicable(t)).toBe(false);
    }
  });

  it("is case-insensitive and null-safe", () => {
    expect(cdcTransportApplicable("MySQL")).toBe(true);
    expect(cdcTransportApplicable(null)).toBe(false);
    expect(cdcTransportApplicable(undefined)).toBe(false);
  });
});
