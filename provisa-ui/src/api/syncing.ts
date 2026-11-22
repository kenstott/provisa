// Copyright (c) 2026 Kenneth Stott
// Canary: 2197dc14-b082-48c8-9ce0-ef7c66a227f4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

// --- The Syncing page (REQ-2247, REQ-2122, REQ-1072, REQ-1074) ---

/** The kinds of outside system an entry is. */
export type SyncEntryType = "data_catalog" | "ontology" | "custom";
export const SYNC_ENTRY_TYPES: SyncEntryType[] = ["data_catalog", "ontology", "custom"];

/** Which way an entry syncs: Export is Provisa to the system, Import the system to Provisa. */
export type SyncDirection = "export" | "import" | "both";
export const SYNC_DIRECTIONS: SyncDirection[] = ["export", "import", "both"];

export const exportsTo = (direction: SyncDirection) =>
  direction === "export" || direction === "both";

export const importsFrom = (direction: SyncDirection) =>
  direction === "import" || direction === "both";

/** Why an Import run held a difference, or left it to Export: a code the page renders through
 * i18n (`syncingTab.holdReason_<code>`), never English text from the server (REQ-2247). */
export const HOLD_REASON_CODES = [
  "no_choice",
  "held_by_choice",
  "edited_in_provisa",
  "never_exchanged",
  "not_importable",
  "left_to_export",
] as const;
export type HoldReasonCode = (typeof HOLD_REASON_CODES)[number];

/** One difference an Import run applied, or held with its reason code and that code's parameters. */
export interface ImportRunItem {
  kind: string;
  object: string;
  reason_code?: HoldReasonCode;
  params?: Record<string, string>;
}

/** One recorded Import run: started by its schedule or by Run import now (REQ-2247). */
export interface ImportRun {
  trigger: "schedule" | "manual";
  outcome: "ok" | "failed";
  applied: ImportRunItem[];
  held: ImportRunItem[];
  /** Why a failed run failed. */
  error: string | null;
  started_at: string;
  finished_at: string;
}

/** What one publish did to one entry. */
export interface PublishOutcome {
  id: string;
  provider: string;
  ok: boolean;
  published: Record<string, number>;
  total_published: number;
  errors: { asset: string; message: string }[];
}

/** One entry as the server reports it.
 *
 *  Credentials appear only as `*_set` booleans: the server never returns a stored secret, so
 *  the form has nothing to render into a password field and nothing to send back unchanged.
 *  Only a data catalog has a provider, a connection and export settings.
 */
export interface SyncEntry {
  id: string;
  type: SyncEntryType;
  direction: SyncDirection;
  provider: string;
  endpoint: string;
  auth_mode: string;
  username: string;
  entra_tenant_id: string;
  entra_client_id: string;
  gcp_project: string;
  gcp_location: string;
  aws_region: string;
  datazone_domain_id: string;
  timeout_seconds: number;
  /** The domains that work with this entry. */
  domains: string[];
  /** REQ-2152: the role a data catalog is published as -- it is sent what that role reads. */
  role: string | null;
  /** The DataZone project that owns the data products published to a DataZone catalog. */
  datazone_project_id: string;
  /** Whether a Unity Catalog catalog also gets each product as a Delta Share. */
  delta_share: boolean;
  /** REQ-2247: the crontab of the entry's Import; empty is no schedule. Only an entry that
   *  imports has one. */
  import_cron: string;
  api_key_set: boolean;
  token_set: boolean;
  entra_client_secret_set: boolean;
  google_service_account_json_set: boolean;
  aws_access_key_id_set: boolean;
  aws_secret_access_key_set: boolean;
  /** The last publish this server ran to it, or null when it has not published to it. */
  last_run: PublishOutcome | null;
  /** The entry's last recorded Import run, or null when none was run. */
  last_import: ImportRun | null;
}

export interface SyncingState {
  /** Whether the organization's plan includes publishing. */
  entitled: boolean;
  required_tier: string;
  providers: string[];
  /** The providers not yet proven against a live instance, which are labelled "(beta)". */
  beta_providers: string[];
  /** REQ-2152: the roles an entry may be published as -- those whose reads the caller holds. */
  publishable_roles: string[];
  /** The organisation-wide schedule of the full publish to every entry that exports. */
  reconcile_cron: string;
  entries: SyncEntry[];
}

export type SyncSecretField =
  | "api_key"
  | "token"
  | "entra_client_secret"
  | "google_service_account_json"
  | "aws_access_key_id"
  | "aws_secret_access_key";

export const SYNC_SECRET_FIELDS: SyncSecretField[] = [
  "api_key",
  "token",
  "entra_client_secret",
  "google_service_account_json",
  "aws_access_key_id",
  "aws_secret_access_key",
];

/** An entry as saved: its settings, and only the credentials the administrator typed. A
 *  credential omitted keeps the one stored for that entry; sent empty, it is cleared. */
export type SyncEntryUpdate = Omit<
  SyncEntry,
  `${SyncSecretField}_set` | "last_run" | "last_import"
> &
  Partial<Record<SyncSecretField, string>>;

/** An entry as the server holds it, ready to send back unchanged. */
export function entryUpdate(entry: SyncEntry): SyncEntryUpdate {
  const kept: Record<string, unknown> = { ...entry };
  delete kept.last_run;
  delete kept.last_import;
  for (const field of SYNC_SECRET_FIELDS) delete kept[`${field}_set`];
  return kept as SyncEntryUpdate;
}

/** The direction a new entry of `type` starts with: a data catalog is published to; an ontology's
 *  releases are uploaded and a custom system is read from until the administrator says otherwise. */
export const startingDirection = (type: SyncEntry["type"]): SyncDirection =>
  type === "data_catalog" ? "export" : "import";

/** A new entry of `type`, empty but for what every entry starts with. */
export const blankEntry = (type: SyncEntry["type"]): SyncEntry => ({
  id: "",
  type,
  direction: startingDirection(type),
  provider: "",
  endpoint: "",
  auth_mode: "api_key",
  username: "",
  entra_tenant_id: "",
  entra_client_id: "",
  gcp_project: "",
  gcp_location: "",
  aws_region: "",
  datazone_domain_id: "",
  timeout_seconds: 30,
  domains: [],
  role: null,
  datazone_project_id: "",
  delta_share: false,
  import_cron: "",
  api_key_set: false,
  token_set: false,
  entra_client_secret_set: false,
  google_service_account_json_set: false,
  aws_access_key_id_set: false,
  aws_secret_access_key_set: false,
  last_run: null,
  last_import: null,
});

/** The name a duplicate is offered under: editable, and refused on save if it exists. */
export const duplicateName = (id: string) => `${id}-copy`;

/**
 * The create form's entry for a duplicate of `entry`: its settings under a new name. No
 * credential goes with it (the server never returns one), and no result of a publish to it.
 */
export function duplicateEntry(entry: SyncEntry): SyncEntry {
  const copy: SyncEntry = {
    ...entry,
    id: duplicateName(entry.id),
    domains: [...entry.domains],
    last_run: null,
    last_import: null,
  };
  for (const field of SYNC_SECRET_FIELDS) copy[`${field}_set`] = false;
  return copy;
}

/** Whether `entry` holds a credential, which its duplicate has to be given again. */
export const holdsCredential = (entry: SyncEntry) =>
  SYNC_SECRET_FIELDS.some((field) => entry[`${field}_set`]);

export interface EntryHealth {
  id: string;
  provider: string;
  ok: boolean;
  error?: string;
}

/** The server's refusal, worded by its code where this build has the wording. */
async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

/** A save the server refused: its reason, and the entry and field it names (`import_cron`). */
export class SyncingRefused extends Error {
  readonly entry: string | null;
  readonly field: string | null;

  constructor(message: string, entry: string | null, field: string | null) {
    super(message);
    this.entry = entry;
    this.field = field;
  }
}

export async function fetchSyncing(): Promise<SyncingState> {
  return ok(await fetch(`${API_BASE}/admin/syncing`), "Syncing fetch");
}

/** The whole list is sent: an entry left out of it is removed. The schedule is kept when sent. */
export async function setSyncing(body: {
  entries: SyncEntryUpdate[];
  reconcile_cron?: string;
}): Promise<{ success: boolean; entries: string[] }> {
  const res = await fetch(`${API_BASE}/admin/syncing`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    const params = (data?.params ?? {}) as Record<string, unknown>;
    throw new SyncingRefused(
      serverMessage(data, requestFailed("Syncing update", res.status)),
      typeof params.entry === "string" ? params.entry : null,
      typeof params.field === "string" ? params.field : null,
    );
  }
  return res.json() as Promise<{ success: boolean; entries: string[] }>;
}

export async function checkEntry(id: string): Promise<EntryHealth> {
  return ok(
    await fetch(`${API_BASE}/admin/syncing/${encodeURIComponent(id)}/health`, {
      method: "POST",
    }),
    "Connection test",
  );
}

export async function publishEntry(
  id: string,
): Promise<{ ok: boolean; entries: PublishOutcome[] }> {
  return ok(
    await fetch(`${API_BASE}/admin/syncing/${encodeURIComponent(id)}/publish`, {
      method: "POST",
    }),
    "Publish",
  );
}

/** What Run import now did: what it applied, held and left to Export, and the run recorded. */
export interface ImportResult {
  id: string;
  applied: ImportRunItem[];
  held: ImportRunItem[];
  left: ImportRunItem[];
  last_import: ImportRun;
}

export async function runImport(id: string): Promise<ImportResult> {
  return ok(
    await fetch(`${API_BASE}/admin/syncing/${encodeURIComponent(id)}/import`, {
      method: "POST",
    }),
    "Import",
  );
}

// --- An entry's differences with its catalog, and its standing choices (REQ-2247) ---

/** The kinds of difference, in the order the review lists them. */
export type DiffKind =
  "description" | "column_description" | "owner" | "tag" | "glossary_link" | "term_definition";
export const DIFF_KINDS: DiffKind[] = [
  "description",
  "column_description",
  "owner",
  "tag",
  "glossary_link",
  "term_definition",
];

/** The kinds a catalog's value can be taken into Provisa for; the server refuses the others. */
export const IMPORTABLE_KINDS: DiffKind[] = [
  "description",
  "column_description",
  "term_definition",
];

/** Which way a value goes: Export sends Provisa's to the system, Import takes the system's. */
export type ExchangeDirection = "export" | "import";
export const EXCHANGE_DIRECTIONS: ExchangeDirection[] = ["export", "import"];

/** The side a decision takes: Provisa's value, or the catalog's. */
export type DiffSide = "provisa" | "catalog";

/** A value as Syncing compares it: a description, or a sorted list of names. */
export type DiffValue = string | string[];

export interface DiffRow {
  kind: DiffKind;
  /** The table or term the object belongs to. */
  group: string;
  object: string;
  provisa: DiffValue;
  catalog: DiffValue;
  last_exchanged: {
    value: DiffValue;
    direction: ExchangeDirection;
    decided_by: string;
    at: string;
  } | null;
  /** Which side moved off the value last exchanged; null when none was exchanged yet. */
  changed: "provisa" | "catalog" | "both" | null;
}

/** A standing choice: take Provisa's value, take the other system's, or hold. */
export type StandingChoice = "provisa" | "catalog" | "hold";
export const STANDING_CHOICES: StandingChoice[] = ["provisa", "catalog", "hold"];

export interface DiffKindGroup {
  kind: DiffKind;
  count: number;
  standing_choice: Record<ExchangeDirection, StandingChoice | null>;
  groups: { group: string; count: number; rows: DiffRow[] }[];
}

export interface EntryReview {
  id: string;
  provider: string;
  /** Which sides a decision may take: Provisa's is an Export, the catalog's an Import. */
  directions: Record<ExchangeDirection, boolean>;
  total: number;
  kinds: DiffKindGroup[];
}

/** One decision: a side for a row (kind and object) or for a group (kind and table or term). */
export type DiffDecision =
  | { side: DiffSide; kind: DiffKind; object: string }
  | { side: DiffSide; kind: DiffKind; group: string };

export interface ApplyResult {
  id: string;
  applied: Record<DiffSide, number>;
  publish: { ok: boolean; entries: PublishOutcome[] } | null;
}

export interface EntryStandingChoices {
  id: string;
  directions: Record<ExchangeDirection, boolean>;
  /** Per direction, the choice of each kind that has one. */
  standing_choices: Record<ExchangeDirection, Partial<Record<DiffKind, StandingChoice>>>;
}

const entryUrl = (id: string, tail: string) =>
  `${API_BASE}/admin/syncing/${encodeURIComponent(id)}/${tail}`;

export async function fetchReview(id: string): Promise<EntryReview> {
  return ok(await fetch(entryUrl(id, "review")), "Review");
}

export async function applyReview(id: string, decisions: DiffDecision[]): Promise<ApplyResult> {
  return ok(
    await fetch(entryUrl(id, "review/apply"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ decisions }),
    }),
    "Accept",
  );
}

export async function fetchStandingChoices(id: string): Promise<EntryStandingChoices> {
  return ok(await fetch(entryUrl(id, "standing-choices")), "Standing choices");
}

/** Sets the entry's standing choice for one direction and kind; null clears it. */
export async function setStandingChoice(
  id: string,
  direction: ExchangeDirection,
  kind: DiffKind,
  choice: StandingChoice | null,
): Promise<EntryStandingChoices> {
  return ok(
    await fetch(entryUrl(id, "standing-choices"), {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ direction, kind, choice }),
    }),
    "Standing choice",
  );
}
