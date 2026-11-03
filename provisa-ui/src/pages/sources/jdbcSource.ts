// Copyright (c) 2026 Kenneth Stott
// Canary: 19949357-b03b-4303-906c-acbda6e03126
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2191: a JDBC source's settings as the form holds them. They are the source's
// federation hints (provisa/jdbc_source/settings.py): "url", "sql_dialect", and per engine
// "capabilities.<engine>", a comma list of the capabilities turned on there. The form picks a
// driver first -- a standard one, whose URL it builds from host, port and database (or file), or
// the admin's own jar -- and holds that pick in "driver", which is not saved: a saved source's
// driver is read back from its URL.

import type { JdbcDriver } from "../../hooks/useJdbcSource";

export const JDBC_SOURCE = "jdbc";
export const ATTACH = "attach";

const URL_KEY = "url";
const DIALECT_KEY = "sql_dialect";
// A jdbc: URL's driver (provisa/jdbc_source/jdbc.py): self-hosted installs only.
export const JDBC_JAR_KEY = "jdbc_driver_jar";
export const JDBC_CLASS_KEY = "jdbc_driver_class";
const JDBC_KEYS = new Set([JDBC_JAR_KEY, JDBC_CLASS_KEY]);

/** Whether the URL names a customer's JDBC driver. */
export function isJdbc(url: string): boolean {
  return url.trim().toLowerCase().startsWith("jdbc:");
}
const CAPABILITIES_PREFIX = "capabilities.";

export function capabilitiesKey(engine: string): string {
  return `${CAPABILITIES_PREFIX}${engine}`;
}

/** The capabilities turned on for the source on `engine`. */
export function capabilitiesOn(fields: Record<string, string>, engine: string): Set<string> {
  const listed = fields[capabilitiesKey(engine)] ?? "";
  return new Set(
    listed
      .split(",")
      .map((c) => c.trim())
      .filter(Boolean),
  );
}

/** `fields` with the capabilities on `engine` replaced by `on`; none on drops the setting. */
export function withCapabilities(
  fields: Record<string, string>,
  engine: string,
  on: Set<string>,
): Record<string, string> {
  const next = { ...fields };
  const key = capabilitiesKey(engine);
  if (on.size === 0) delete next[key];
  else next[key] = [...on].sort().join(",");
  return next;
}

/** The form's pick of driver (not saved). */
export const DRIVER_KEY = "driver";
/** The pick of the admin's own JDBC driver, a jar. */
export const JAR_DRIVER = "jar";

/** The backend a URL's scheme names, without its driver (`mysql+pymysql://` -> `mysql`). */
function backendOf(url: string): string {
  if (!url.includes("://")) return "";
  return url.split("://")[0].split("+")[0].toLowerCase();
}

/** The driver picked: the form's pick, else the one the saved URL names (`""` for none). */
export function driverOf(fields: Record<string, string>): string {
  if (fields[DRIVER_KEY]) return fields[DRIVER_KEY];
  const url = fields[URL_KEY] ?? "";
  return isJdbc(url) ? JAR_DRIVER : backendOf(url);
}

/** What a standard driver's URL is built from. */
export interface UrlParts {
  host: string;
  port: string;
  database: string;
  /** A file database's file (SQLite, DuckDB). */
  file: string;
  /** The URL's driver options, kept as written. */
  query: string;
}

/** The parts of a standard driver's `url`. */
export function urlParts(url: string, driver: JdbcDriver): UrlParts {
  const parts: UrlParts = { host: "", port: "", database: "", file: "", query: "" };
  if (!url.includes("://")) return parts;
  let rest = url.slice(url.indexOf("://") + 3);
  const q = rest.indexOf("?");
  if (q >= 0) {
    parts.query = rest.slice(q + 1);
    rest = rest.slice(0, q);
  }
  if (driver.file) {
    parts.file = rest.startsWith("/") ? rest.slice(1) : rest;
    return parts;
  }
  const slash = rest.indexOf("/");
  const authority = slash >= 0 ? rest.slice(0, slash) : rest;
  parts.database = slash >= 0 ? rest.slice(slash + 1) : "";
  const colon = authority.lastIndexOf(":");
  parts.host = colon >= 0 ? authority.slice(0, colon) : authority;
  parts.port = colon >= 0 ? authority.slice(colon + 1) : "";
  return parts;
}

/** A standard driver's URL, built from its parts: `<scheme>:///<file>` for a file database,
 * else `<scheme>://<host>[:<port>]/<database>`. It carries no user or password: the source's own
 * fields do. */
export function buildUrl(driver: JdbcDriver, parts: UrlParts): string {
  const query = parts.query ? `?${parts.query}` : "";
  if (driver.file) return `${driver.scheme}:///${parts.file}${query}`;
  const port = parts.port ? `:${parts.port}` : "";
  return `${driver.scheme}://${parts.host}${port}/${parts.database}${query}`;
}

/** Whether the picked driver's fields are filled, so Detect can run. */
export function driverFilled(fields: Record<string, string>, driver: JdbcDriver | null): boolean {
  const url = fields[URL_KEY] ?? "";
  if (driverOf(fields) === JAR_DRIVER) {
    return isJdbc(url) && !!fields[JDBC_JAR_KEY]?.trim() && !!fields[JDBC_CLASS_KEY]?.trim();
  }
  if (!driver) return false;
  const parts = urlParts(url, driver);
  return driver.file ? !!parts.file.trim() : !!parts.host.trim();
}

/** The federation hints a JDBC source is saved with: its settings, as the form holds them. */
export function jdbcSourceHints(fields: Record<string, string>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(fields).filter(
      ([k, v]) =>
        v !== "" &&
        (k === URL_KEY ||
          k === DIALECT_KEY ||
          k.startsWith(CAPABILITIES_PREFIX) ||
          (JDBC_KEYS.has(k) && isJdbc(fields[URL_KEY] ?? ""))),
    ),
  );
}
