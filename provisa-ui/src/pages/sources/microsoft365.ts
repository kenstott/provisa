// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: a Microsoft 365 source's setup. What the operator chooses rides in the source's
// mapping, under the names provisa/microsoft365/settings.py reads; that module is the
// authority on what is valid, and this one only says what is still missing from the form.

export const MICROSOFT_365 = "microsoft_365";
/** Where an administrator enters the organisation's Microsoft client. */
export const MAIL_PLATFORMS_ROUTE = "/admin/email";

const GRAPH = "https://graph.microsoft.com/";
/** What Microsoft is asked for: reading the mailbox, a standing approval, and who approved. */
export const M365_SCOPES = [`${GRAPH}Mail.Read`, "offline_access", `${GRAPH}User.Read`];

const stated = (fields: Record<string, string>, key: string) => (fields[key] ?? "").trim();

/** What must be entered before Microsoft can be asked for the owner's approval. */
export function m365ConnectMissing(fields: Record<string, string>, sourceId: string): string[] {
  const missing: string[] = [];
  if (!sourceId.trim()) missing.push("source_id");
  if (!stated(fields, "m365_account")) missing.push("m365_account");
  return missing;
}

/** The source's mapping for what the fields say. The organisation's Microsoft client is not
 * part of it: a source keeps only its owner's approval. */
export function m365Mapping(fields: Record<string, string>): Record<string, unknown> {
  return {
    accounts: [stated(fields, "m365_account")],
    resources: ["mail"],
    refresh_token: stated(fields, "m365_refresh_token"),
  };
}

/** The fields a stored source's mapping carries, for the form that edits it. */
export function m365FieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, unknown>;
  const fields: Record<string, string> = {
    m365_account: ((mapping.accounts as string[] | undefined) ?? []).join(""),
  };
  if (mapping.refresh_token !== undefined) {
    fields.m365_refresh_token = String(mapping.refresh_token);
  }
  if (fields.m365_refresh_token) fields.m365_connected_as = fields.m365_account;
  return fields;
}
