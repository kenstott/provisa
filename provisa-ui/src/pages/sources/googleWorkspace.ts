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

import { SignInRefused, shapesOffered, type SignInStatus } from "../../lib/sourceSignIn";
import { fieldValue, mappingKeysOf } from "./descriptor";
import { CALENDAR, MAIL, resourcesField, resourcesOf } from "./mailResources";

export const GOOGLE_WORKSPACE = "google_workspace";
// REQ-2114: the mapping keys and the sign-in a source of one mailbox records are the descriptor's
// (provisa/api/admin/source_types.py, from provisa/google_workspace/settings.py).
const K = mappingKeysOf(GOOGLE_WORKSPACE, [
  "accounts",
  "resources",
  "mailboxes",
  "sign_in",
  "refresh_token",
  "calendar_send_updates",
  "mail_content",
  "mail_labels",
  "mail_include_spam_trash",
  "mail_search",
  "mail_since",
] as const);
/** Where an administrator enters the organisation's Google client. */
export const MAIL_PLATFORMS_ROUTE = "/admin/email";

export const GW_GOOGLE_ACCOUNT = fieldValue(GOOGLE_WORKSPACE, "mappingJson", K.sign_in) as string;
export const GW_ONE = "one";
export const GW_ORGANISATION = "organisation";
export const GW_EVERYONE = "everyone";
export const GW_GROUP = "group";
export const GW_LIST = "list";
export const GW_MAIL_FULL = "full";
export const GW_MAIL_HEADERS = "headers";

const lines = (text: string | undefined) =>
  (text ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

const stated = (fields: Record<string, string>, key: string) => (fields[key] ?? "").trim();

/** What the source reads: mail, calendars, tasks (REQ-1994). What Google is asked to grant
 * is the server's to say, from the source's mapping (`gwMapping`). */
export const gwResources = (fields: Record<string, string>) => resourcesOf(fields, "gw");

/** Whether the source reads mail, and so states how much of it. */
const readsMail = (fields: Record<string, string>) => gwResources(fields).includes(MAIL);

/** Whether mail is read as headers and labels only, which Google does not search. */
export const gwHeadersOnly = (fields: Record<string, string>) =>
  stated(fields, "gw_mail_content") === GW_MAIL_HEADERS;

/** Whether the source reads the organisation's mailboxes, with the organisation's own
 * credential, and not one mailbox its owner connects. */
export const gwOrganisation = (fields: Record<string, string>) =>
  stated(fields, "gw_whose") === GW_ORGANISATION;

/** Which of the two a Google Workspace source's form offers and a new source opens on
 * (`shapesOffered`). One mailbox is signed in to with the organisation's client (`configured`);
 * the organisation's mailboxes are read with its service account once that is allowed
 * (`organisation_mailboxes`) -- an entry may have either without the other. */
export const gwShapes = (entry: SignInStatus | null, saved: string) =>
  shapesOffered(entry, saved, {
    one: entry?.configured === true,
    organisation: entry?.organisation_mailboxes === true,
  });

/** Which mailboxes of the organisation, as the source's mapping states it; null while the
 * form does not yet say. */
export function gwMailboxes(fields: Record<string, string>): Record<string, unknown> | null {
  const choice = stated(fields, "gw_choice");
  if (choice === GW_EVERYONE) return { everyone: true };
  if (choice === GW_GROUP) {
    return stated(fields, "gw_group") ? { group: stated(fields, "gw_group") } : null;
  }
  if (choice === GW_LIST) {
    return lines(fields.gw_list).length ? { list: lines(fields.gw_list) } : null;
  }
  return null;
}

/** What must be entered before Google can be asked for the owner's approval. */
export function gwConnectMissing(fields: Record<string, string>, sourceId: string): string[] {
  const missing: string[] = [];
  if (!sourceId.trim()) missing.push("source_id");
  if (!stated(fields, "gw_account")) missing.push("gw_account");
  return [...missing, ...gwReadsMissing(fields)];
}

/** What the source reads, still unsaid: at least one resource, and how much mail. */
function gwReadsMissing(fields: Record<string, string>): string[] {
  if (gwResources(fields).length === 0) return ["gw_resources"];
  return readsMail(fields) && !stated(fields, "gw_mail_content") ? ["gw_mail_content"] : [];
}

/** The fields still needed before the source can be saved. */
export function gwMissing(fields: Record<string, string>): string[] {
  if (gwOrganisation(fields)) {
    const missing: string[] = [];
    if (gwMailboxes(fields) === null) missing.push("gw_choice");
    return [...missing, ...gwReadsMissing(fields)];
  }
  const missing: string[] = [];
  if (!stated(fields, "gw_account")) missing.push("gw_account");
  missing.push(...gwReadsMissing(fields));
  // Its owner has signed in: the server keeps the approval and says so (the form asks it).
  if (!stated(fields, "gw_connected_as")) missing.push("gw_connected_as");
  return missing;
}

/** The source's mapping for what the fields say. The organisation's Google credentials are
 * not part of it: a source of one mailbox keeps only its owner's approval, and a source of the
 * organisation's mailboxes keeps only which ones. */
export function gwMapping(fields: Record<string, string>): Record<string, unknown> {
  const resources = gwResources(fields);
  const mapping: Record<string, unknown> = gwOrganisation(fields)
    ? { [K.resources]: resources, [K.mailboxes]: gwMailboxes(fields) ?? {} }
    : {
        [K.accounts]: [stated(fields, "gw_account")],
        [K.resources]: resources,
        [K.sign_in]: GW_GOOGLE_ACCOUNT,
      };
  // The owner's approval is kept by the server, not in the source. A source connected before
  // that still names it here; it is carried through unchanged until the server has taken it.
  if (!gwOrganisation(fields) && stated(fields, "gw_earlier_approval")) {
    mapping[K.refresh_token] = stated(fields, "gw_earlier_approval");
  }
  // REQ-1994: whether a change to an event tells its attendees; off unless ticked.
  if (resources.includes(CALENDAR)) {
    mapping[K.calendar_send_updates] = fields.gw_send_updates === "true";
  }
  if (!resources.includes(MAIL)) return mapping;
  mapping[K.mail_content] = stated(fields, "gw_mail_content");
  if (lines(fields.gw_mail_labels).length) mapping[K.mail_labels] = lines(fields.gw_mail_labels);
  if (fields.gw_mail_include_spam_trash === "true") mapping[K.mail_include_spam_trash] = true;
  if (!gwHeadersOnly(fields)) {
    if (stated(fields, "gw_mail_search")) mapping[K.mail_search] = stated(fields, "gw_mail_search");
    if (stated(fields, "gw_mail_since")) mapping[K.mail_since] = stated(fields, "gw_mail_since");
  }
  return mapping;
}

/** The fields a stored source's mapping carries, for the form that edits it. */
export function gwFieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, unknown>;
  const fields: Record<string, string> = {
    gw_mail_labels: ((mapping[K.mail_labels] as string[] | undefined) ?? []).join("\n"),
    gw_mail_include_spam_trash: mapping[K.mail_include_spam_trash] ? "true" : "",
    ...resourcesField("gw", (mapping[K.resources] as string[] | undefined) ?? []),
    gw_send_updates: mapping[K.calendar_send_updates] === true ? "true" : "",
  };
  for (const key of [K.mail_content, K.mail_search, K.mail_since]) {
    if (mapping[key] !== undefined) fields[`gw_${key}`] = String(mapping[key]);
  }
  const chosen = mapping[K.mailboxes] as Record<string, unknown> | undefined;
  if (chosen !== undefined) {
    fields.gw_whose = GW_ORGANISATION;
    if (chosen.everyone === true) fields.gw_choice = GW_EVERYONE;
    if (typeof chosen.group === "string") {
      fields.gw_choice = GW_GROUP;
      fields.gw_group = chosen.group;
    }
    if (Array.isArray(chosen.list)) {
      fields.gw_choice = GW_LIST;
      fields.gw_list = (chosen.list as string[]).join("\n");
    }
    return fields;
  }
  fields.gw_whose = GW_ONE;
  fields.gw_account = ((mapping[K.accounts] as string[] | undefined) ?? []).join("");
  const approval = mapping[K.refresh_token];
  if (typeof approval === "string" && approval !== "") {
    fields.gw_earlier_approval = approval;
    fields.gw_connected_as = fields.gw_account; // connected the earlier way
  }
  return fields;
}

/** How many mailboxes a choice names in the organisation's directory now. A refusal carries
 * the code its message is localised by. */
export async function gwCheckMailboxes(mailboxes: Record<string, unknown>): Promise<number> {
  const answer = await fetch("/admin/google-workspace/mailboxes/check", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mailboxes }),
  });
  const said = await answer.json();
  if (!answer.ok) throw new SignInRefused(said);
  return (said as { count: number }).count;
}
