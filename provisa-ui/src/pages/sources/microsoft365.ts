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

import { SignInRefused, shapesOffered, type SignInStatus } from "../../lib/sourceSignIn";
import { mappingKeysOf } from "./descriptor";
import { CALENDAR, MAIL, resourcesField, resourcesOf } from "./mailResources";

export const MICROSOFT_365 = "microsoft_365";
// REQ-2114: the mapping keys are the descriptor's (provisa/api/admin/source_types.py, from
// provisa/microsoft365/settings.py).
const K = mappingKeysOf(MICROSOFT_365, [
  "accounts",
  "resources",
  "mailboxes",
  "refresh_token",
  "mail_since",
  "calendar_check_days",
  "calendar_send_updates",
] as const);
/** Where an administrator enters the organisation's Microsoft client. */
export const MAIL_PLATFORMS_ROUTE = "/admin/email";

/** What the source reads: mail, calendars, tasks (REQ-1994). What Microsoft is asked to grant
 * is the server's to say, from the source's mapping (`m365Mapping`). */
export const m365Resources = (fields: Record<string, string>) => resourcesOf(fields, "m365");

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

/** Which of the two a Microsoft 365 source's form offers and a new source opens on
 * (`shapesOffered`). The organisation's client serves both: it signs one mailbox's owner in, and
 * reads the organisation's mailboxes once that is allowed -- so only the switch tells them apart. */
export const m365Shapes = (entry: SignInStatus | null, saved: string) =>
  shapesOffered(entry, saved, {
    one: entry?.configured === true,
    organisation: entry?.configured === true && entry.organisation_mailboxes === true,
  });

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
  return [...missing, ...m365ReadsMissing(fields)];
}

/** What the source reads, still unsaid: at least one resource, and the days a check for
 * changed events looks at when it reads calendars. */
export function m365ReadsMissing(fields: Record<string, string>): string[] {
  const resources = m365Resources(fields);
  if (resources.length === 0) return ["m365_resources"];
  return resources.includes(CALENDAR) && !(Number(stated(fields, "m365_check_days")) >= 1)
    ? ["m365_check_days"]
    : [];
}

/** The source's mapping for what the fields say. The organisation's Microsoft client is not
 * part of it: a source of one mailbox keeps only its owner's approval, and a source of the
 * organisation's mailboxes keeps only which ones. */
export function m365Mapping(fields: Record<string, string>): Record<string, unknown> {
  // Nothing received before this date is held, whichever mailboxes are read; left out, mail
  // of any age is. The same setting, by the same name, as a Google Workspace source's.
  const resources = m365Resources(fields);
  const since =
    resources.includes(MAIL) && stated(fields, "m365_mail_since")
      ? { [K.mail_since]: stated(fields, "m365_mail_since") }
      : {};
  // REQ-1994: a source that reads calendars says how many days a check looks at, and whether a
  // change Microsoft would announce to attendees is made.
  const calendar = resources.includes(CALENDAR)
    ? {
        [K.calendar_check_days]: Number(stated(fields, "m365_check_days")),
        [K.calendar_send_updates]: fields.m365_send_updates === "true",
      }
    : {};
  if (m365Organisation(fields)) {
    return {
      [K.resources]: resources,
      [K.mailboxes]: m365Mailboxes(fields) ?? {},
      ...since,
      ...calendar,
    };
  }
  const mapping: Record<string, unknown> = {
    [K.accounts]: [stated(fields, "m365_account")],
    [K.resources]: resources,
    ...since,
    ...calendar,
  };
  // The owner's approval is kept by the server, not in the source. A source connected before
  // that still names it here; it is carried through unchanged until the server has taken it.
  if (stated(fields, "m365_earlier_approval")) {
    mapping[K.refresh_token] = stated(fields, "m365_earlier_approval");
  }
  return mapping;
}

/** The fields a stored source's mapping carries, for the form that edits it. */
export function m365FieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, unknown>;
  const chosen = mapping[K.mailboxes] as Record<string, unknown> | undefined;
  const mailSince = mapping[K.mail_since];
  const checkDays = mapping[K.calendar_check_days];
  const since: Record<string, string> = {
    ...(typeof mailSince === "string" ? { m365_mail_since: mailSince } : {}),
    ...resourcesField("m365", (mapping[K.resources] as string[] | undefined) ?? []),
    m365_send_updates: mapping[K.calendar_send_updates] === true ? "true" : "",
    ...(typeof checkDays === "number" ? { m365_check_days: String(checkDays) } : {}),
  };
  if (chosen !== undefined) {
    const fields: Record<string, string> = { m365_whose: M365_ORGANISATION, ...since };
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
    ...since,
    m365_whose: M365_ONE,
    m365_account: ((mapping[K.accounts] as string[] | undefined) ?? []).join(""),
  };
  const approval = mapping[K.refresh_token];
  if (typeof approval === "string" && approval !== "") {
    fields.m365_earlier_approval = approval;
    fields.m365_connected_as = fields.m365_account; // connected the earlier way
  }
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
