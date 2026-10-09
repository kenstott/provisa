// Copyright (c) 2026 Kenneth Stott
// Canary: d2ee0de3-b8e6-4d9a-8c03-5cef781945fe
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: a Google Workspace source's setup. What the operator chooses rides in the source's
// mapping, under the names provisa/google_workspace/settings.py reads; that module is the
// authority on what is valid, and this one only says what is still missing from the form.

export const GOOGLE_WORKSPACE = "google_workspace";

export const GW_GOOGLE_ACCOUNT = "google_account";
export const GW_SERVICE_ACCOUNT = "service_account";
export const GW_MAIL_FULL = "full";
export const GW_MAIL_HEADERS = "headers";

const SCOPE = "https://www.googleapis.com/auth/";
const MAIL_SCOPE: Record<string, string> = {
  [GW_MAIL_FULL]: `${SCOPE}gmail.readonly`,
  [GW_MAIL_HEADERS]: `${SCOPE}gmail.metadata`,
};

const lines = (text: string | undefined) =>
  (text ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

const stated = (fields: Record<string, string>, key: string) => (fields[key] ?? "").trim();

/** What Google is asked for: the scope of what the source reads and of nothing else. */
export function gwScopes(fields: Record<string, string>): string[] {
  const scope = MAIL_SCOPE[stated(fields, "gw_mail_content")];
  return scope ? [scope] : [];
}

/** Whether mail is read as headers and labels only, which Google does not search. */
export const gwHeadersOnly = (fields: Record<string, string>) =>
  stated(fields, "gw_mail_content") === GW_MAIL_HEADERS;

/** What the operator must enter before Google can be asked for their approval. */
export function gwConnectMissing(fields: Record<string, string>, sourceId: string): string[] {
  const missing: string[] = [];
  if (!sourceId.trim()) missing.push("source_id");
  for (const key of ["gw_account", "gw_mail_content", "gw_client_id", "gw_client_secret"]) {
    if (!stated(fields, key)) missing.push(key);
  }
  return missing;
}

/** The fields still needed before the source can be saved. */
export function gwMissing(fields: Record<string, string>): string[] {
  const missing: string[] = [];
  for (const key of ["gw_account", "gw_sign_in", "gw_mail_content"]) {
    if (!stated(fields, key)) missing.push(key);
  }
  const signIn = stated(fields, "gw_sign_in");
  if (signIn === GW_GOOGLE_ACCOUNT) {
    for (const key of ["gw_client_id", "gw_client_secret", "gw_refresh_token"]) {
      if (!stated(fields, key)) missing.push(key);
    }
  }
  if (signIn === GW_SERVICE_ACCOUNT && !stated(fields, "gw_service_account_key")) {
    missing.push("gw_service_account_key");
  }
  return missing;
}

/** The source's mapping for what the fields say. */
export function gwMapping(fields: Record<string, string>): Record<string, unknown> {
  const signIn = stated(fields, "gw_sign_in");
  const mapping: Record<string, unknown> = {
    accounts: [stated(fields, "gw_account")],
    resources: ["mail"],
    sign_in: signIn,
    mail_content: stated(fields, "gw_mail_content"),
  };
  const credentials =
    signIn === GW_GOOGLE_ACCOUNT
      ? ["client_id", "client_secret", "refresh_token"]
      : ["service_account_key"];
  for (const key of credentials) mapping[key] = stated(fields, `gw_${key}`);
  if (lines(fields.gw_mail_labels).length) mapping.mail_labels = lines(fields.gw_mail_labels);
  if (fields.gw_mail_include_spam_trash === "true") mapping.mail_include_spam_trash = true;
  if (!gwHeadersOnly(fields)) {
    if (stated(fields, "gw_mail_search")) mapping.mail_search = stated(fields, "gw_mail_search");
    if (stated(fields, "gw_mail_since")) mapping.mail_since = stated(fields, "gw_mail_since");
  }
  return mapping;
}

/** The fields a stored source's mapping carries, for the form that edits it. */
export function gwFieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, unknown>;
  const fields: Record<string, string> = {
    gw_account: ((mapping.accounts as string[] | undefined) ?? []).join(""),
    gw_mail_labels: ((mapping.mail_labels as string[] | undefined) ?? []).join("\n"),
    gw_mail_include_spam_trash: mapping.mail_include_spam_trash ? "true" : "",
  };
  for (const key of [
    "sign_in",
    "mail_content",
    "mail_search",
    "mail_since",
    "client_id",
    "client_secret",
    "refresh_token",
    "service_account_key",
  ]) {
    if (mapping[key] !== undefined) fields[`gw_${key}`] = String(mapping[key]);
  }
  if (fields.gw_refresh_token) fields.gw_connected_as = fields.gw_account;
  return fields;
}
