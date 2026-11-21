// Copyright (c) 2026 Kenneth Stott
// Canary: d9c69b0b-b43c-47cf-bb91-13deb497e183
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984, REQ-1993, REQ-2200, REQ-2227: a code source -- the organisation's own Node, Python,
// Go or Rust functions, run in a sandboxed worker -- keeps what its code is in its mapping
// (provisa/codesource/source.py):
//
//   runtime   node | bun | deno (Node); python (Python); go (Go); rust (Rust)
//   code      a package at an exact version, a Git repository at a commit, or an uploaded archive
//   lockfile  a lockfile given beside code that carries none
//   entry     a Python source's entry module, when its code has more than one; a Go source's
//             package directory, when its functions are not the module's root package; a Rust
//             source's crate directory, when its functions are not in the code's root crate
//   mode      REQ-2098: dev (its code on this server) or built (its image on a remote host); a
//             built source also keeps its remote host and the code hash it is pinned to, which
//             an edit of the form carries through unchanged
//   secrets   REQ-2104: the names its code may read through Provisa's services, each a vault
//             reference (the names alone when each is the org secret of that name)
//   llm       REQ-2105: {cap_usd}, the monthly cap on what its calls to the LLM service cost
//   settings  REQ-2106: its non-secret settings, name -> value, which its code reads
//   workers   REQ-2131: how many workers it runs in each Provisa process (1 when not written);
//             a column transform's batches spread across them
//
// The content hash of the code is pinned by the server when the source is saved.

export const NODE = "node";
export const PYTHON = "python";
export const GO = "go";
export const RUST = "rust";
export const CODE_SOURCE_TYPES = new Set([NODE, PYTHON, GO, RUST]);

/** The languages whose source names where its code is entered: Python's module, Go's package,
 * Rust's crate. */
export const NAMED_ENTRY = new Set([PYTHON, GO, RUST]);

export const NODE_RUNTIMES = ["node", "bun", "deno"] as const;

/** Where the code comes from: a package (npm, PyPI, a Go module or a crate), a Git repository, or an
 * upload. */
export type CodeOrigin = "package" | "git" | "upload" | "directory";

/** The package kind each language's packages come from. */
export const PACKAGE_KIND: Record<string, string> = {
  [NODE]: "npm",
  [PYTHON]: "pypi",
  [GO]: "gomod",
  [RUST]: "cargo",
};

/** The lockfiles each language installs from (provisa/node/install.py, provisa/python/install.py,
 * provisa/golang/install.py, provisa/rustlang/install.py). */
export const LOCKFILES: Record<string, string[]> = {
  [NODE]: ["package-lock.json", "npm-shrinkwrap.json", "bun.lock", "bun.lockb", "deno.lock"],
  [PYTHON]: ["uv.lock", "pylock.toml", "requirements.txt"],
  [GO]: ["go.sum"],
  [RUST]: ["Cargo.lock"],
};

const COMMIT = /^[0-9a-f]{40}$/;

/** REQ-2104: the source's declared secrets, from the form's `code_secrets`: each name with the
 * reference typed for it, or null for the org secret of that name. The form spells no reference
 * of its own: a name picked from the org's secrets stays a name. */
export function declaredSecrets(fields: Record<string, string>): Record<string, string | null> {
  return fields.code_secrets
    ? (JSON.parse(fields.code_secrets) as Record<string, string | null>)
    : {};
}

/** The `secrets` a mapping stores: the names alone when each is the org secret of that name,
 * else each name with its reference or null (an `${env:...}` one is kept as written). */
function secretsMapping(
  declared: Record<string, string | null>,
): string[] | Record<string, string | null> | null {
  const names = Object.keys(declared);
  if (names.length === 0) return null;
  return names.every((n) => declared[n] === null) ? names : declared;
}

/** The form's `code_secrets` with the org secrets `names` declared, keeping any reference a
 * declared name already has. */
export function withSecretNames(fields: Record<string, string>, names: string[]): string {
  const held = declaredSecrets(fields);
  return JSON.stringify(Object.fromEntries(names.map((n) => [n, held[n] ?? null])));
}

/** REQ-2106: the source's non-secret settings from the form's `code_settings`, one `name = value`
 * to a line; a blank line is skipped. */
export function settingsOf(text: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const line of text.split("\n")) {
    const at = line.indexOf("=");
    const name = (at < 0 ? line : line.slice(0, at)).trim();
    if (name) out[name] = at < 0 ? "" : line.slice(at + 1).trim();
  }
  return out;
}

/** An uploaded archive: its content hash, and the name of the file it was uploaded from when the
 * form has it, so the form shows what it is. */
function uploadedCode(fields: Record<string, string>): Record<string, string> {
  const filename = (fields.code_upload_filename ?? "").trim();
  return { kind: "upload", upload: fields.code_upload ?? "", ...(filename ? { filename } : {}) };
}

/** The source's mapping, as the server stores it, from the form's fields. */
export function codeMappingJson(type: string, fields: Record<string, string>): string {
  const origin = (fields.code_origin ?? "package") as CodeOrigin;
  const code =
    origin === "package"
      ? {
          kind: PACKAGE_KIND[type],
          package: (fields.code_package ?? "").trim(),
          version: (fields.code_version ?? "").trim(),
        }
      : origin === "git"
        ? {
            kind: "git",
            repository: (fields.code_repository ?? "").trim(),
            commit: (fields.code_commit ?? "").trim(),
            subdirectory: (fields.code_subdirectory ?? "").trim(),
          }
        : origin === "directory"
          ? { kind: "directory", path: (fields.code_directory ?? "").trim() }
          : uploadedCode(fields);
  const lockfile = fields.lockfile_upload
    ? { filename: fields.lockfile_filename, upload: fields.lockfile_upload }
    : null;
  const entry = (fields.code_entry ?? "").trim();
  const secrets = secretsMapping(declaredSecrets(fields));
  const cap = (fields.code_llm_cap ?? "").trim();
  // A connector's source keeps its values whole: one may be multi-line text.
  const held = fields.code_connector ? { connector: fields.code_connector } : {};
  const settings = fields.code_connector
    ? (JSON.parse(fields.code_connector_settings || "{}") as Record<string, unknown>)
    : settingsOf(fields.code_settings ?? "");
  // REQ-2131: a mapping that names no workers runs one, so one is not written.
  const workers = Number((fields.code_workers ?? "").trim() || "1");
  const built =
    fields.code_mode === "built"
      ? {
          mode: "built",
          remote: JSON.parse(fields.code_remote || "{}") as Record<string, unknown>,
          content_hash: fields.code_content_hash ?? "",
        }
      : {};
  return JSON.stringify({
    // Node names its runtime; Python, Go and Rust each have one.
    runtime: type === NODE ? (fields.code_runtime ?? "node") : type,
    code,
    ...(lockfile ? { lockfile } : {}),
    ...(NAMED_ENTRY.has(type) && entry ? { entry } : {}),
    ...(secrets ? { secrets } : {}),
    ...(cap ? { llm: { cap_usd: Number(cap) } } : {}),
    ...(Object.keys(settings).length ? { settings } : {}),
    ...(workers !== 1 ? { workers } : {}),
    ...built,
    ...held,
  });
}

/** The form's fields from a stored mapping. */
export function codeFieldsFromMapping(mappingJson: string): Record<string, string> {
  const m = JSON.parse(mappingJson) as {
    runtime?: string;
    code?: Record<string, string>;
    lockfile?: { filename: string; upload: string } | null;
    entry?: string;
    secrets?: string[] | Record<string, string | null>;
    llm?: { cap_usd?: number };
    settings?: Record<string, string | number | boolean>;
    workers?: number;
    mode?: string;
    remote?: Record<string, unknown>;
    content_hash?: string;
    connector?: string;
  };
  const code = m.code ?? {};
  const secrets = Array.isArray(m.secrets)
    ? Object.fromEntries(m.secrets.map((n) => [n, null]))
    : (m.secrets ?? {});
  const origin: CodeOrigin =
    code.kind === "git"
      ? "git"
      : code.kind === "upload"
        ? "upload"
        : code.kind === "directory"
          ? "directory"
          : "package";
  return {
    code_runtime: m.runtime ?? "node",
    code_origin: origin,
    code_package: code.package ?? "",
    code_version: code.version ?? "",
    code_repository: code.repository ?? "",
    code_commit: code.commit ?? "",
    code_subdirectory: code.subdirectory ?? "",
    code_upload: code.upload ?? "",
    code_upload_filename: code.filename ?? "",
    code_directory: code.kind === "directory" ? (code.path ?? "") : "",
    lockfile_upload: m.lockfile?.upload ?? "",
    lockfile_filename: m.lockfile?.filename ?? "",
    code_entry: m.entry ?? "",
    code_secrets: Object.keys(secrets).length ? JSON.stringify(secrets) : "",
    code_llm_cap: m.llm?.cap_usd === undefined ? "" : String(m.llm.cap_usd),
    code_settings: m.connector
      ? ""
      : Object.entries(m.settings ?? {})
          .map(([name, value]) => `${name} = ${String(value)}`)
          .join("\n"),
    ...(m.connector
      ? { code_connector: m.connector, code_connector_settings: JSON.stringify(m.settings ?? {}) }
      : {}),
    // REQ-2131: a mapping that names no workers runs one.
    code_workers: String(m.workers ?? 1),
    code_mode: m.mode ?? "dev",
    code_remote: m.mode === "built" ? JSON.stringify(m.remote ?? {}) : "",
    code_content_hash: m.content_hash ?? "",
  };
}

/** The fields the form cannot be saved without, by the field's key. */
export function missingCodeFields(fields: Record<string, string>): string[] {
  const origin = (fields.code_origin ?? "package") as CodeOrigin;
  const needed =
    origin === "package"
      ? ["code_package", "code_version"]
      : origin === "git"
        ? ["code_repository", "code_commit"]
        : origin === "directory"
          ? ["code_directory"]
          : ["code_upload"];
  const missing = needed.filter((k) => !(fields[k] ?? "").trim());
  if (origin === "git" && fields.code_commit && !COMMIT.test(fields.code_commit.trim())) {
    missing.push("code_commit");
  }
  return missing;
}

const GITHUB = /^(?:https:\/\/github\.com\/|git@github\.com:)([^/]+)\/([^/]+?)(?:\.git)?\/?$/;

/** REQ-2121: where the source's code opens in VS Code: a server directory in the installed VS
 * Code (a vscode://file link), a Git repository on vscode.dev at its commit; null for a package,
 * an upload, or a Git host vscode.dev does not open. */
export function vscodeLink(fields: Record<string, string>): string | null {
  const origin = (fields.code_origin ?? "package") as CodeOrigin;
  if (origin === "directory") {
    const path = (fields.code_directory ?? "").trim();
    return path ? `vscode://file/${path.replace(/^\/+/, "")}` : null;
  }
  if (origin === "git") {
    const repo = GITHUB.exec((fields.code_repository ?? "").trim());
    const commit = (fields.code_commit ?? "").trim();
    if (!repo || !commit) return null;
    const sub = (fields.code_subdirectory ?? "").trim().replace(/^\/+|\/+$/g, "");
    return `https://vscode.dev/github/${repo[1]}/${repo[2]}/tree/${commit}${sub ? `/${sub}` : ""}`;
  }
  return null;
}
