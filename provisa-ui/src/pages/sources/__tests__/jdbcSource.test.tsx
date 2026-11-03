// Copyright (c) 2026 Kenneth Stott
// Canary: be70a13e-e40a-419c-bc01-ca3108f71d9c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2191: a JDBC source is its connection URL and the SQL dialect Provisa writes, with the
// capabilities turned on for the active engine; Detect ticks those its checks passed, shows why the
// others failed, and saves nothing.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor } from "../../../test-utils/render";
import { JdbcSourceFields } from "../JdbcSourceFields";
import { SOURCE_TYPES } from "../constants";
import { attachAllowed, capabilitiesOn, jdbcSourceHints, withCapabilities } from "../jdbcSource";
import type { SourceFormState } from "../SourceFormFields";

const detect = vi.fn();
vi.mock("../../../hooks/useJdbcSource", () => ({
  useJdbcSourceSetup: () => ({
    setup: {
      engine: "duckdb",
      sqlDialects: ["postgres", "sqlite", "tsql", "ansi"],
      capabilities: ["attach", "predicate", "join", "aggregate", "key_range"],
    },
    error: undefined,
  }),
  useDetectJdbcSource: () => ({ detect, loading: false }),
}));

describe("the source pick list", () => {
  it("offers JDBC among the relational sources", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "jdbc")).toMatchObject({
      label: "JDBC",
      category: "RDBMS",
    });
  });
});

describe("what a JDBC source saves", () => {
  it("is its URL, its dialect and the capabilities on each engine, and nothing else", () => {
    const fields = {
      url: "sqlite:///{files}/shop.db",
      sql_dialect: "sqlite",
      "capabilities.duckdb": "join,predicate",
      scratch: "x",
    };
    expect(jdbcSourceHints(fields)).toEqual({
      url: "sqlite:///{files}/shop.db",
      sql_dialect: "sqlite",
      "capabilities.duckdb": "join,predicate",
    });
  });

  it("holds each engine's capabilities apart; none on drops the setting", () => {
    const fields = withCapabilities({}, "duckdb", new Set(["predicate"]));
    expect(capabilitiesOn(fields, "duckdb")).toEqual(new Set(["predicate"]));
    expect(capabilitiesOn(fields, "trino")).toEqual(new Set());
    expect(withCapabilities(fields, "duckdb", new Set())).toEqual({});
  });

  it("keeps a JDBC driver's jar and class only for a jdbc: URL", () => {
    const driver = { jdbc_driver_jar: "{files}/d.jar", jdbc_driver_class: "com.v.Driver" };
    expect(jdbcSourceHints({ url: "jdbc:vendor://h/db", sql_dialect: "ansi", ...driver })).toEqual({
      url: "jdbc:vendor://h/db",
      sql_dialect: "ansi",
      ...driver,
    });
    expect(
      jdbcSourceHints({ url: "postgresql://h/db", sql_dialect: "postgres", ...driver }),
    ).toEqual({ url: "postgresql://h/db", sql_dialect: "postgres" });
  });

  it("offers attach only for a postgresql or mysql URL", () => {
    expect(attachAllowed("postgresql+psycopg2://h/db")).toBe(true);
    expect(attachAllowed("mysql+pymysql://h/db")).toBe(true);
    expect(attachAllowed("mssql+pyodbc://h/db")).toBe(false);
    expect(attachAllowed("sqlite:///{files}/a.db")).toBe(false);
    expect(attachAllowed("postgresql")).toBe(false);
    expect(attachAllowed("jdbc:postgresql://h/db")).toBe(false);
  });
});

function Harness({ onFields }: { onFields: (f: Record<string, string>) => void }) {
  const [form, setForm] = useState({ username: "", password: "" } as SourceFormState);
  const [fields, setFields] = useState<Record<string, string>>({});
  return (
    <JdbcSourceFields
      form={form}
      setForm={setForm}
      authFields={fields}
      setAuthFields={(f) => {
        onFields(f);
        setFields(f);
      }}
    />
  );
}

describe("the JDBC fields", () => {
  it("tick what Detect passed for the active engine and say why the rest failed", async () => {
    detect.mockResolvedValue({
      engine: "duckdb",
      checks: [
        { capability: "attach", passed: false, reason: "not built yet" },
        { capability: "predicate", passed: true, reason: "" },
        { capability: "join", passed: false, reason: "OperationalError: no such function" },
        { capability: "aggregate", passed: true, reason: "" },
        { capability: "key_range", passed: true, reason: "" },
      ],
    });
    const onFields = vi.fn();
    renderInForm(<Harness onFields={onFields} />);
    fireEvent.change(screen.getByTestId("jdbc-source-url-input"), {
      target: { value: "postgresql+psycopg2://db.example.com/sales" },
    });
    // Nothing is checked until Detect is pressed.
    expect(detect).not.toHaveBeenCalled();
    expect(screen.getByTestId("jdbc-source-detect")).toBeDisabled();
    fireEvent.click(screen.getByTestId("jdbc-source-dialect-select"));
    fireEvent.click(await screen.findByRole("option", { name: "postgres" }));
    expect(
      (screen.getByTestId("jdbc-source-capability-predicate") as HTMLInputElement).checked,
    ).toBe(false);
    fireEvent.click(screen.getByTestId("jdbc-source-detect"));
    await waitFor(() => expect(screen.getByTestId("jdbc-source-detect-results")).toBeTruthy());
    expect(detect).toHaveBeenCalledWith({
      url: "postgresql+psycopg2://db.example.com/sales",
      sqlDialect: "postgres",
      username: "",
      password: "",
      jdbcDriverJar: "",
      jdbcDriverClass: "",
    });
    expect(onFields.mock.lastCall?.[0]["capabilities.duckdb"]).toBe(
      "aggregate,key_range,predicate",
    );
    expect(screen.getByTestId("jdbc-source-check-join").textContent).toContain(
      "OperationalError: no such function",
    );
    // Live attach is offered for a postgresql URL, but cannot be turned on yet.
    expect(screen.getByTestId("jdbc-source-capability-attach")).toBeDisabled();
    // A changed URL or dialect is another source to the checks: press Detect again.
    fireEvent.change(screen.getByTestId("jdbc-source-url-input"), {
      target: { value: "postgresql+psycopg2://db.example.com/other" },
    });
    expect(screen.queryByTestId("jdbc-source-detect-results")).toBeNull();
  });

  it("ask a jdbc: URL's driver jar and class, and offer it no attach", () => {
    renderInForm(<Harness onFields={vi.fn()} />);
    expect(screen.queryByTestId("jdbc-source-jdbc-jar-input")).toBeNull();
    fireEvent.change(screen.getByTestId("jdbc-source-url-input"), {
      target: { value: "jdbc:vendor://db.example.com:1521/sales" },
    });
    expect(screen.getByTestId("jdbc-source-jdbc-jar-input")).toBeTruthy();
    expect(screen.getByTestId("jdbc-source-jdbc-class-input")).toBeTruthy();
    expect(screen.queryByTestId("jdbc-source-capability-attach")).toBeNull();
    expect(screen.getByTestId("jdbc-source-capability-predicate")).toBeTruthy();
  });
});
