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
// capabilities turned on for the active engine. The form picks a driver first: a standard one,
// whose URL it builds from host, port and database (or file), or the admin's own jar, uploaded
// from their machine. Detect ticks the capabilities its checks passed, shows why the others
// failed, and saves nothing. No engine is named on the form.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor } from "../../../test-utils/render";
import { JdbcSourceFields } from "../JdbcSourceFields";
import { SOURCE_TYPES } from "../constants";
import {
  buildUrl,
  capabilitiesOn,
  driverOf,
  jdbcSourceHints,
  urlParts,
  withCapabilities,
} from "../jdbcSource";
import type { SourceFormState } from "../SourceFormFields";

const POSTGRESQL = {
  backend: "postgresql",
  sourceType: "postgresql",
  scheme: "postgresql",
  file: false,
  attach: true,
};
const MYSQL = {
  backend: "mysql",
  sourceType: "mysql",
  scheme: "mysql+pymysql",
  file: false,
  attach: false,
};
const SQLITE = {
  backend: "sqlite",
  sourceType: "sqlite",
  scheme: "sqlite",
  file: true,
  attach: true,
};

const detect = vi.fn();
vi.mock("../../../hooks/useJdbcSource", () => ({
  useJdbcSourceSetup: () => ({
    setup: {
      engine: "duckdb",
      drivers: [POSTGRESQL, MYSQL, SQLITE],
      sqlDialects: ["postgres", "sqlite", "tsql", "ansi"],
      capabilities: ["attach", "predicate", "join", "aggregate", "key_range"],
    },
    error: undefined,
  }),
  useDetectJdbcSource: () => ({ detect, loading: false }),
}));

const upload = vi.fn();
vi.mock("../sourceUpload", async (actual) => ({
  ...(await actual<typeof import("../sourceUpload")>()),
  uploadSourceFile: (...args: unknown[]) => upload(...args),
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

  it("builds a standard driver's URL from its parts, and reads them back", () => {
    const parts = { host: "db.example.com", port: "5432", database: "sales", file: "", query: "" };
    const url = buildUrl(MYSQL, parts);
    expect(url).toBe("mysql+pymysql://db.example.com:5432/sales");
    expect(urlParts(url, MYSQL)).toEqual(parts);
    expect(buildUrl(MYSQL, { ...parts, port: "" })).toBe("mysql+pymysql://db.example.com/sales");
    // A file database is named by its file; driver options are kept as written.
    const file = "sqlite:///{files}/shop.db?mode=ro";
    expect(urlParts(file, SQLITE)).toMatchObject({ file: "{files}/shop.db", query: "mode=ro" });
    expect(buildUrl(SQLITE, urlParts(file, SQLITE))).toBe(file);
  });

  it("reads a saved source's driver back from its URL, and saves no pick of one", () => {
    expect(driverOf({ url: "mysql+pymysql://h/db" })).toBe("mysql");
    expect(driverOf({ url: "jdbc:vendor://h/db" })).toBe("jar");
    expect(driverOf({})).toBe("");
    expect(driverOf({ driver: "jar", url: "" })).toBe("jar");
    expect(jdbcSourceHints({ driver: "mysql", url: "mysql://h/db", sql_dialect: "mysql" })).toEqual(
      { url: "mysql://h/db", sql_dialect: "mysql" },
    );
  });
});

const fieldsOf = (onFields: ReturnType<typeof vi.fn>) =>
  onFields.mock.lastCall?.[0] as Record<string, string>;

async function pick(testId: string, name: string) {
  fireEvent.click(screen.getByTestId(testId));
  fireEvent.click(await screen.findByRole("option", { name }));
}

function Harness({ onFields }: { onFields: (f: Record<string, string>) => void }) {
  const [form, setForm] = useState({ id: "vendor", username: "", password: "" } as SourceFormState);
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
  it("pick a standard driver first, named by its database, and build its URL", async () => {
    const onFields = vi.fn();
    renderInForm(<Harness onFields={onFields} />);
    // The first field is the driver; nothing asks for a URL.
    expect(screen.queryByTestId("jdbc-source-url-input")).toBeNull();
    expect(screen.queryByTestId("jdbc-source-host-input")).toBeNull();
    fireEvent.click(screen.getByTestId("jdbc-source-driver-select"));
    for (const name of ["PostgreSQL", "MySQL", "SQLite", "Your own JDBC driver (jar)"]) {
      expect(await screen.findByRole("option", { name })).toBeTruthy();
    }
    fireEvent.click(screen.getByRole("option", { name: "MySQL" }));
    fireEvent.change(screen.getByTestId("jdbc-source-host-input"), {
      target: { value: "db.example.com" },
    });
    fireEvent.change(screen.getByTestId("jdbc-source-port-input"), { target: { value: "3307" } });
    fireEvent.change(screen.getByTestId("jdbc-source-database-input"), {
      target: { value: "sales" },
    });
    expect(fieldsOf(onFields).url).toBe("mysql+pymysql://db.example.com:3307/sales");
    // A file database asks for its file instead.
    await pick("jdbc-source-driver-select", "SQLite");
    expect(screen.queryByTestId("jdbc-source-host-input")).toBeNull();
    fireEvent.change(screen.getByTestId("jdbc-source-file-input"), {
      target: { value: "{files}/shop.db" },
    });
    expect(fieldsOf(onFields).url).toBe("sqlite:///{files}/shop.db");
  });

  it("name no engine, and offer live attach where the active engine attaches the driver's type", async () => {
    renderInForm(<Harness onFields={vi.fn()} />);
    await pick("jdbc-source-driver-select", "PostgreSQL");
    expect(screen.getByLabelText("Live attach")).toBeEnabled();
    expect(screen.getByLabelText("Filter pushdown")).toBeTruthy();
    expect(screen.getByLabelText("Join pushdown")).toBeTruthy();
    expect(screen.getByLabelText("Aggregate pushdown")).toBeTruthy();
    expect(screen.getByLabelText("Key-range pushdown")).toBeTruthy();
    // The engine the settings are held for is internal: the form never names it.
    expect(document.body.textContent).not.toMatch(/duckdb/i);
    // The engine cannot attach a MySQL source: no attach is offered for it.
    await pick("jdbc-source-driver-select", "MySQL");
    expect(screen.queryByTestId("jdbc-source-capability-attach")).toBeNull();
  });

  it("tick what Detect passed once the driver's fields are filled, and say why the rest failed", async () => {
    detect.mockResolvedValue({
      engine: "duckdb",
      checks: [
        { capability: "attach", passed: true, reason: "" },
        { capability: "predicate", passed: true, reason: "" },
        { capability: "join", passed: false, reason: "OperationalError: no such function" },
        { capability: "aggregate", passed: true, reason: "" },
        { capability: "key_range", passed: true, reason: "" },
      ],
    });
    const onFields = vi.fn();
    renderInForm(<Harness onFields={onFields} />);
    await pick("jdbc-source-dialect-select", "postgres");
    expect(screen.getByTestId("jdbc-source-detect")).toBeDisabled();
    await pick("jdbc-source-driver-select", "PostgreSQL");
    expect(screen.getByTestId("jdbc-source-detect")).toBeDisabled();
    fireEvent.change(screen.getByTestId("jdbc-source-host-input"), {
      target: { value: "db.example.com" },
    });
    fireEvent.change(screen.getByTestId("jdbc-source-database-input"), {
      target: { value: "sales" },
    });
    // Nothing is checked until Detect is pressed.
    expect(detect).not.toHaveBeenCalled();
    expect(
      (screen.getByTestId("jdbc-source-capability-predicate") as HTMLInputElement).checked,
    ).toBe(false);
    fireEvent.click(screen.getByTestId("jdbc-source-detect"));
    await waitFor(() => expect(screen.getByTestId("jdbc-source-detect-results")).toBeTruthy());
    expect(detect).toHaveBeenCalledWith({
      url: "postgresql://db.example.com/sales",
      sqlDialect: "postgres",
      username: "",
      password: "",
      jdbcDriverJar: "",
      jdbcDriverClass: "",
    });
    expect(fieldsOf(onFields)["capabilities.duckdb"]).toBe("aggregate,attach,key_range,predicate");
    expect((screen.getByTestId("jdbc-source-capability-attach") as HTMLInputElement).checked).toBe(
      true,
    );
    expect(screen.getByTestId("jdbc-source-check-join").textContent).toContain(
      "OperationalError: no such function",
    );
    // A changed connection or dialect is another source to the checks: press Detect again.
    fireEvent.change(screen.getByTestId("jdbc-source-database-input"), {
      target: { value: "other" },
    });
    expect(screen.queryByTestId("jdbc-source-detect-results")).toBeNull();
  });

  it("take the admin's own jar as a file uploaded with the source, with its class and jdbc: URL, and offer it no attach", async () => {
    upload.mockResolvedValue({ path: "{files}/vendor/vendor-jdbc.jar", bytes: 3 });
    const onFields = vi.fn();
    const { container } = renderInForm(<Harness onFields={onFields} />);
    await pick("jdbc-source-dialect-select", "ansi");
    await pick("jdbc-source-driver-select", "Your own JDBC driver (jar)");
    expect(screen.queryByTestId("jdbc-source-host-input")).toBeNull();
    expect(screen.queryByTestId("jdbc-source-capability-attach")).toBeNull();
    expect(screen.getByTestId("jdbc-source-capability-predicate")).toBeTruthy();
    const jar = new File(["jar"], "vendor-jdbc.jar");
    const input = container.querySelector('input[type="file"]') as HTMLInputElement;
    expect(input.accept).toBe(".jar");
    fireEvent.change(input, { target: { files: [jar] } });
    await waitFor(() =>
      expect(fieldsOf(onFields).jdbc_driver_jar).toBe("{files}/vendor/vendor-jdbc.jar"),
    );
    expect(upload).toHaveBeenCalledWith("vendor", "jdbc", jar, expect.any(Function));
    expect(screen.getByTestId("jdbc-source-detect")).toBeDisabled();
    fireEvent.change(screen.getByTestId("jdbc-source-jdbc-class-input"), {
      target: { value: "com.vendor.jdbc.Driver" },
    });
    fireEvent.change(screen.getByTestId("jdbc-source-url-input"), {
      target: { value: "jdbc:vendor://db.example.com:1521/sales" },
    });
    expect(screen.getByTestId("jdbc-source-detect")).toBeEnabled();
    expect(jdbcSourceHints(fieldsOf(onFields))).toEqual({
      url: "jdbc:vendor://db.example.com:1521/sales",
      sql_dialect: "ansi",
      jdbc_driver_jar: "{files}/vendor/vendor-jdbc.jar",
      jdbc_driver_class: "com.vendor.jdbc.Driver",
    });
  });

  it("show a refused jar upload by the server's own words", async () => {
    upload.mockRejectedValue(
      new Error(
        "a customer's JDBC driver runs as code in Provisa's own JVM, which only a self-hosted install allows",
      ),
    );
    const { container } = renderInForm(<Harness onFields={vi.fn()} />);
    await pick("jdbc-source-driver-select", "Your own JDBC driver (jar)");
    const input = container.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [new File(["jar"], "d.jar")] } });
    expect((await screen.findByTestId("source-file-upload-error")).textContent).toContain(
      "self-hosted",
    );
  });
});
