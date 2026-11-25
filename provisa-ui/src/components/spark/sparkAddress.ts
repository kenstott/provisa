// Copyright (c) 2026 Kenneth Stott
// Canary: 5f0c2a7e-91d3-4b8a-a6e4-2c7d9b13e580
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2225 (amended 2026-11-24): a Spark cluster address as structured fields. The fields compose
// the one stored Spark Connect address, sc://host:port/;name=value;..., and a pasted address is
// split back into them. The server parses the address it is given (provisa/core/spark_cluster.py)
// and refuses a malformed one by name on save.

// Requirements: REQ-2225

/** Spark Connect's own port, which the form starts with. */
export const DEFAULT_SPARK_PORT = "15002";

const SCHEME = "sc://";
const REFERENCE = /^\$\{secret:[A-Za-z0-9_.\-/]+\}$/;
const PARAM = /^[A-Za-z_][A-Za-z0-9_]*=[^;]*$/;
/** The parameters the fields hold; any other is carried through as it was given. */
const HELD = new Set(["use_ssl", "user_id", "token"]);

export interface ClusterFields {
  host: string;
  port: string;
  tls: boolean;
  userId: string;
  /** The token as a ${secret:NAME} reference; empty for a cluster that takes none. */
  token: string;
  /** Other Spark Connect parameters of a pasted address, in order, kept unchanged. */
  extra: [string, string][];
}

export const emptyCluster = (): ClusterFields => ({
  host: "",
  port: DEFAULT_SPARK_PORT,
  tls: false,
  userId: "",
  token: "",
  extra: [],
});

export const isSecretReference = (value: string): boolean => REFERENCE.test(value.trim());

/** A host name or address the fields accept: no scheme, port, path, credential or space. */
const goodHost = (host: string) => /^[A-Za-z0-9._-]+$|^\[[0-9A-Fa-f:.]+\]$/.test(host);

/** What is wrong with the fields, by field; empty when they compose an address. A blank host is
 * not wrong: it is a cluster left unset. */
export function clusterProblems(
  f: ClusterFields,
): Partial<Record<"host" | "port" | "token", string>> {
  const out: Partial<Record<"host" | "port" | "token", string>> = {};
  if (!f.host.trim()) return out;
  if (!goodHost(f.host.trim())) out.host = "host";
  const port = Number(f.port.trim());
  if (!/^\d+$/.test(f.port.trim()) || port < 1 || port > 65535) out.port = "port";
  if (f.token.trim() && !isSecretReference(f.token)) out.token = "token";
  return out;
}

/** The one Spark Connect address the fields compose; empty when no host is given (the cluster is
 * left unset). `withToken`: the token reference is a parameter of the address (a source's own
 * cluster); without it the token is kept beside the address (the organisation's default). */
export function composeAddress(f: ClusterFields, withToken: boolean): string {
  const host = f.host.trim();
  if (!host) return "";
  const params: [string, string][] = [];
  // Spark Connect sends a token over TLS only, so a token turns TLS on.
  if (f.tls || (withToken && f.token.trim())) params.push(["use_ssl", "true"]);
  if (f.userId.trim()) params.push(["user_id", f.userId.trim()]);
  params.push(...f.extra);
  if (withToken && f.token.trim()) params.push(["token", f.token.trim()]);
  const tail = params.length ? `/;${params.map(([k, v]) => `${k}=${v}`).join(";")}` : "";
  return `${SCHEME}${host}:${f.port.trim() || DEFAULT_SPARK_PORT}${tail}`;
}

/** An address that is not a Spark Connect address, said with why. */
export class NotSparkAddress extends Error {
  readonly reason: "scheme" | "host" | "port" | "param";
  readonly part: string;
  constructor(reason: "scheme" | "host" | "port" | "param", part: string) {
    super(`${reason}: ${part}`);
    this.name = "NotSparkAddress";
    this.reason = reason;
    this.part = part;
  }
}

/** A Spark Connect address split into the fields. A token found in it is returned apart, never
 * put in a field unless it is a secret reference already: a value is to be kept as a secret. */
export function splitAddress(address: string): {
  fields: ClusterFields;
  tokenValue: string | null;
} {
  const text = address.trim();
  if (!text.startsWith(SCHEME)) throw new NotSparkAddress("scheme", text);
  const rest = text.slice(SCHEME.length);
  const slash = rest.indexOf("/");
  const head = slash < 0 ? rest : rest.slice(0, slash);
  const tail = slash < 0 ? "" : rest.slice(slash + 1);
  // A credential in the authority is refused, as the server refuses it: the token is a parameter.
  if (head.includes("@")) throw new NotSparkAddress("host", head);
  const bracket = head.startsWith("[") ? head.indexOf("]") : -1;
  const colon = bracket >= 0 ? head.indexOf(":", bracket) : head.lastIndexOf(":");
  const host = colon < 0 ? head : head.slice(0, colon);
  const port = colon < 0 ? DEFAULT_SPARK_PORT : head.slice(colon + 1);
  if (!host || !goodHost(host)) throw new NotSparkAddress("host", head);
  const portNumber = Number(port);
  if (!/^\d+$/.test(port) || portNumber < 1 || portNumber > 65535)
    throw new NotSparkAddress("port", port);
  const fields = { ...emptyCluster(), host, port };
  let tokenValue: string | null = null;
  for (const part of tail.split(";").filter(Boolean)) {
    if (!PARAM.test(part)) throw new NotSparkAddress("param", part);
    const at = part.indexOf("=");
    const [name, value] = [part.slice(0, at), part.slice(at + 1)];
    if (name === "use_ssl") fields.tls = value.toLowerCase() === "true";
    else if (name === "user_id") fields.userId = value;
    else if (name === "token") {
      if (isSecretReference(value)) fields.token = value;
      else tokenValue = value;
    } else if (!HELD.has(name)) fields.extra.push([name, value]);
  }
  return { fields, tokenValue };
}

/** The fields of a stored address, as kept (its token a reference); the empty fields for none. */
export function storedFields(address: string | null | undefined, token = ""): ClusterFields {
  if (!address?.trim()) return { ...emptyCluster(), token };
  const { fields } = splitAddress(address);
  return token ? { ...fields, token } : fields;
}

/** A secret name for a pasted token: the host's, as a vault name may be written. */
export const tokenSecretName = (host: string): string =>
  `spark_${host.replace(/[^A-Za-z0-9]+/g, "_").replace(/^_+|_+$/g, "")}_token`.toLowerCase();
