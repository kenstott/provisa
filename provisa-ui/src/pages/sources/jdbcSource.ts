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
// "capabilities.<engine>", a comma list of the capabilities turned on there.

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

/** Whether the URL's scheme names a backend with an engine attach (postgresql, mysql). */
export function attachAllowed(url: string): boolean {
  const scheme = url.split("://")[0] ?? "";
  if (!url.includes("://")) return false;
  const backend = scheme.split("+")[0].toLowerCase();
  return backend === "postgresql" || backend === "mysql";
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
