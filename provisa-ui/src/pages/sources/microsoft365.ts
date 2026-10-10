// Copyright (c) 2026 Kenneth Stott
// Canary: ee546dd4-4d5a-4b5a-b093-18fd8f888de2
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

import { SignInRefused } from "../../lib/sourceSignIn";

export const MICROSOFT_365 = "microsoft_365";
/** Where an administrator enters the organisation's Microsoft client. */
export const MAIL_PLATFORMS_ROUTE = "/admin/email";

const GRAPH = "https://graph.microsoft.com/";
/** What Microsoft is asked for: reading the mailbox, a standing approval, and who approved. */
export const M365_SCOPES = [`${GRAPH}Mail.Read`, "offline_access", `${GRAPH}User.Read`];

export const M365_ONE = "one";
export const M365_ORGANISATION = "organisation";
export const M365_EVERYONE = "everyone";
export const M365_GROUP = "group";
export const M365_LIST = "list";

const stated = (fields: Record<string, string>, key: string) => (fields[key] ?? "").trim();

const lines = (text: string | undefined) =>
  (text ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

/** Whether the source reads the organisation's mailboxes, with the organisation's own
 * credential, and not one mailbox its owner connects. */
export const m365Organisation = (fields: Record<string, string>) =>
  stated(fields, "m365_whose") === M365_ORGANISATION;

/** Which mailboxes of the organisation, as the source's mapping states it; null while the
 * form does not yet say. */
export function m365Mailboxes(fields: Record<string, string>): Record<string, unknown> | null {
  const choice = stated(fields, "m365_choice");
  if (choice === M365_EVERYONE) return { everyone: true };
  if (choice === M365_GROUP) {
    return stated(fields, "m365_group") ? { group: stated(fields, "m365_group") } : null;
  }
  if (choice === M365_LIST) {
    return lines(fields.m365_list).length ? { list: lines(fields.m365_list) } : null;
  }
  return null;
}

/** What must be entered before Microsoft can be asked for the owner's approval. */
export function m365ConnectMissing(fields: Record<string, string>, sourceId: string): string[] {
  const missing: string[] = [];
  if (!sourceId.trim()) missing.push("source_id");
  if (!stated(fields, "m365_account")) missing.push("m365_account");
  return missing;
}

/** The source's mapping for what the fields say. The organisation's Microsoft client is not
 * part of it: a source of one mailbox keeps only its owner's approval, and a source of the
 * organisation's mailboxes keeps only which ones. */
export function m365Mapping(fields: Record<string, string>): Record<string, unknown> {
  if (m365Organisation(fields)) {
    return { resources: ["mail"], mailboxes: m365Mailboxes(fields) ?? {} };
  }
  return {
    accounts: [stated(fields, "m365_account")],
    resources: ["mail"],
    refresh_token: stated(fields, "m365_refresh_token"),
  };
}

/** The fields a stored source's mapping carries, for the form that edits it. */
export function m365FieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, unknown>;
  const chosen = mapping.mailboxes as Record<string, unknown> | undefined;
  if (chosen !== undefined) {
    const fields: Record<string, string> = { m365_whose: M365_ORGANISATION };
    if (chosen.everyone === true) fields.m365_choice = M365_EVERYONE;
    if (typeof chosen.group === "string") {
      fields.m365_choice = M365_GROUP;
      fields.m365_group = chosen.group;
    }
    if (Array.isArray(chosen.list)) {
      fields.m365_choice = M365_LIST;
      fields.m365_list = (chosen.list as string[]).join("\n");
    }
    return fields;
  }
  const fields: Record<string, string> = {
    m365_whose: M365_ONE,
    m365_account: ((mapping.accounts as string[] | undefined) ?? []).join(""),
  };
  if (mapping.refresh_token !== undefined) {
    fields.m365_refresh_token = String(mapping.refresh_token);
  }
  if (fields.m365_refresh_token) fields.m365_connected_as = fields.m365_account;
  return fields;
}

/** How many mailboxes a choice names in the organisation's directory now. A refusal carries
 * the code its message is localised by. */
export async function m365CheckMailboxes(mailboxes: Record<string, unknown>): Promise<number> {
  const answer = await fetch("/admin/microsoft-365/mailboxes/check", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mailboxes }),
  });
  const said = await answer.json();
  if (!answer.ok) throw new SignInRefused(said);
  return (said as { count: number }).count;
}
