// Copyright (c) 2026 Kenneth Stott
// Canary: 1a7d3f92-6e4c-4b81-9f05-2c8a5e6d3b71
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Shared helpers for the license-registration Pages Functions (REQ-1793). Not a route itself —
// Pages only turns a file under functions/ into a route if it exports onRequest*, which this
// doesn't.
//
// `canonicalPayload` MUST byte-for-byte match `provisa.licensing.license._canonical_payload`
// (Python: json.dumps(body_sans_sig, sort_keys=True, separators=(",", ":")), default
// ensure_ascii=True) — that Python function is what a Provisa install re-derives to verify the
// signature this module produces. Cross-runtime-verified during development: a payload signed
// here with a PKCS8-imported Ed25519 key round-tripped through `cryptography`'s
// Ed25519PublicKey.verify() on the Python side.

// Escapes one JS string the way Python's json.dumps(..., ensure_ascii=True) does: standard JSON
// control-char escapes, then every code point above 0x7e as \uXXXX (surrogate-paired for astral
// characters), matching CPython's json encoder exactly.
function pyJsonString(s) {
  let out = '"';
  for (const ch of s) {
    const code = ch.codePointAt(0);
    if (ch === '"') out += '\\"';
    else if (ch === "\\") out += "\\\\";
    else if (code === 0x08) out += "\\b";
    else if (code === 0x0c) out += "\\f";
    else if (code === 0x0a) out += "\\n";
    else if (code === 0x0d) out += "\\r";
    else if (code === 0x09) out += "\\t";
    else if (code < 0x20) out += "\\u" + code.toString(16).padStart(4, "0");
    else if (code < 0x7f) out += ch;
    else if (code <= 0xffff) out += "\\u" + code.toString(16).padStart(4, "0");
    else {
      const c = code - 0x10000;
      const hi = 0xd800 + (c >> 10);
      const lo = 0xdc00 + (c & 0x3ff);
      out += "\\u" + hi.toString(16).padStart(4, "0") + "\\u" + lo.toString(16).padStart(4, "0");
    }
  }
  return out + '"';
}

export function canonicalPayload(obj) {
  const keys = Object.keys(obj)
    .filter((k) => k !== "sig")
    .sort();
  const parts = keys.map((k) => {
    const v = obj[k];
    let encoded;
    if (v === null || v === undefined) encoded = "null";
    else if (typeof v === "string") encoded = pyJsonString(v);
    else if (typeof v === "boolean") encoded = v ? "true" : "false";
    else if (typeof v === "number") encoded = String(v);
    else throw new Error(`canonicalPayload: unsupported type for ${k}`);
    return pyJsonString(k) + ":" + encoded;
  });
  return new TextEncoder().encode("{" + parts.join(",") + "}");
}

// `privateKeyPkcs8B64` is the LICENSE_PRIVATE_KEY_PKCS8_B64 secret — the Ed25519 private key,
// PKCS8 DER, base64-encoded. Never logged, never returned to a client.
export async function signLicense(payload, privateKeyPkcs8B64) {
  const der = Uint8Array.from(atob(privateKeyPkcs8B64), (c) => c.charCodeAt(0));
  const key = await crypto.subtle.importKey("pkcs8", der, { name: "Ed25519" }, false, ["sign"]);
  const sig = await crypto.subtle.sign({ name: "Ed25519" }, key, canonicalPayload(payload));
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export function randomToken() {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export function isValidEmail(email) {
  return email.length <= 320 && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

export async function sendEmail(env, { to, subject, html, text, attachments }) {
  const body = {
    from: "Provisa Licensing <license@provisa.dev>",
    to: [to],
    subject,
    text,
  };
  if (html) body.html = html;
  if (attachments) body.attachments = attachments;
  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.RESEND_API_KEY}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`resend send failed: ${res.status} ${await res.text()}`);
  }
}
