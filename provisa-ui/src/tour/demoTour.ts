// Copyright (c) 2026 Kenneth Stott
// Canary: 3532577d-ead2-4eea-b618-d00827c54331
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-2010: a named demo's own tour, the demo's track. Its steps are written with the demo
 * (demo/named/<name>/tour.yaml) and reach the UI on /setup/status; each names a page and one object
 * of the demo, and is turned here into a step of the tour runner. Their words are
 * tour.steps.<key>, like every other step's.
 */
import type { Capability } from "../types/auth";
import type { ExampleQuery } from "../api/setup";
import type { TourStep } from "./tourSteps";
import { runSql } from "../api/admin";
import type { TourPrerequisites } from "./tourPrerequisites";

/** REQ-2013: a demo's track needs the sources and tables its steps show. */
export function demoTourPrerequisites(defs: readonly DemoTourStepDef[]): TourPrerequisites {
  return {
    sources: defs.flatMap((d) => (d.source !== undefined ? [d.source] : [])),
    tables: defs.flatMap((d) => (d.table !== undefined ? [d.table] : [])),
  };
}

/** REQ-2012: a step asking Polly is shown only with the answer stored for it when the demo's
 * template was built; nothing is asked live, so a demo not built from a template leaves it out. */
export function demoStepShown(def: DemoTourStepDef): boolean {
  return def.polly === undefined || def.answer !== undefined;
}

/**
 * What marks a demo's track walked: the demo and a hash of its steps' keys and routes. A revised
 * track hashes differently, so a control plane that walked the old one offers the new one again.
 */
export function demoTrackStamp(name: string, defs: readonly DemoTourStepDef[]): string {
  const text = JSON.stringify(defs.map((d) => [d.key, d.route ?? null]));
  // FNV-1a, 32 bits: a short, stable fingerprint, not a secret.
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return `${name}:${hash.toString(16).padStart(8, "0")}`;
}

/** The page a demo's tour leaves the visitor on, from "Look around on my own": tour.yaml's
 * ``landing``, its route and the capability its gate needs. */
export interface DemoLanding {
  route: string;
  capability: Capability;
}

/** One step of tour.yaml, as the server sends it: its page, and exactly one target. */
export interface DemoTourStepDef {
  key: string;
  route?: string;
  capability?: Capability;
  /** A source, by id: its row on the Sources page. */
  source?: string;
  /** A registered table, as `<source id>.<table name>`: its row on the Tables page. */
  table?: string;
  /** With `table`: its detail panel opened on a section, and in Columns on a tab, as
   *  `<section>[.<tab>]` (`columns.testing`: each column's fake). */
  open?: string;
  /** A relationship, by alias: its row on the Relationships page. */
  relationship?: string;
  /** An example query of the demo, by name: opened in a new GraphiQL tab and run. */
  example?: string;
  /** A statement: opened in a new SQL explorer tab and run. */
  sql?: string;
  /** A REST call, as "<METHOD> <path>[?<params>]": driven in the OpenAPI page and run. */
  api?: string;
  /** A JSON:API request, its path and query: opened on the JSON:API page and run. */
  jsonapi?: string;
  /** A gRPC call, as the gRPC page takes one ("<Method>(by=[...], funcs=[...])"): sent. */
  grpc?: string;
  /** A Cypher query: run in a new frame of the Graph page. */
  cypher?: string;
  /** A property the Graph page's frame on screen is grouped by (`degTotal`): the frame the step
   *  before ran, kept, and grouped. */
  group_by?: string;
  /** A statement: its lineage built on the Lineage page, each column it reads traced back through
   *  the views it reads to the source fields. */
  lineage?: string;
  /** A glossary term, by name: opened on the Glossary page, its definition and links shown. */
  term?: string;
  /** A data product, by id: opened on the Data Products page, its members and lineage shown. */
  product?: string;
  /** A role (or "all"): shown in the SQL explorer's run-as picker, picked on Next. */
  role?: string;
  /** REQ-1163: a time, ISO 8601, set in the SQL explorer's "as of" picker: the statements after
   *  it read bitemporal views as of it. */
  as_of?: string;
  /** REQ-1163: a time, ISO 8601, set in the SQL explorer's "as recorded at" picker: the
   *  statements after it read bitemporal views as Provisa had recorded them then. */
  as_recorded?: string;
  /** REQ-2083: the "as recorded at" time taken from the answer of an earlier `sql` step of the
   *  tour, row `row` (0-based) and column `column`, when this step runs: a recording time Provisa
   *  stamped, which no tour can know in advance. The earlier step's statement is run again as it
   *  ran then (its role and time travel). */
  as_recorded_from?: { step: string; row: number; column: string };
  /** An environment of the model (REQ-2053): shown in the header's environment switcher, picked
   *  on Next, so the steps after it read that environment. */
  env?: string;
  /** REQ-2084: the demo's notebook (notebook.ipynb): its cells added to the Notebook tab's open
   *  notebook and run there, in the visitor's browser. */
  notebook?: true;
  /** A question for Polly: the panel opens on it and its stored `answer`, as an exchange. */
  polly?: string;
  /** REQ-2012: Polly's answer to `polly`, stored when the demo's template was built. */
  answer?: string;
  /** A picture beside the demo's tour.yaml (a PNG), shown in the step's popover on the page the
   *  step before it is on; the server sends it as `image_src`, a data URL. */
  image?: string;
  image_src?: string;
  /** Beside any target: another named demo, by name, its popover's "Open the <title> demo" button
   *  going to `/demos/<name>` (a demo that builds on another links back to it, and on to it). */
  demo?: string;
}

/** Answers that come from the server are slow on a loaded demo; a run's result waits this long. */
const RESULT_WAIT_MS = 60000;
/** A notebook's cells wait for the browser's Python to start and load its packages first. */
const NOTEBOOK_WAIT_MS = 240000;
// The SQL explorer's own run-as picker (REQ-1013), not the app-wide role: it is what the explorer's
// statements run as.
const RUN_AS = '[data-testid="sql-role"]';
// The header's role picker: what every other page's requests run as (the GraphQL page among them).
const APP_ROLE = '[data-testid="role-selector-trigger"]';
// The header's environment switcher: what every request after it is served from.
const ENV_SWITCHER = '[data-testid="env-switcher-trigger"]';
const SQL_RESULT_ROW = '.sql-results-table tbody tr:not([data-testid="results-grid-empty"])';

export function demoTourSteps(
  defs: readonly DemoTourStepDef[],
  examples: readonly ExampleQuery[],
  notebook: readonly string[] = [],
): TourStep[] {
  const steps: TourStep[] = [];
  let onPage: string | undefined;
  // What each `sql` step ran as: the SQL explorer's role and time travel set by the steps before
  // it (REQ-2083), so a later step can run its statement again as it ran then.
  const ran = new Map<string, StatementRun>();
  const context: Omit<StatementRun, "sql"> = {};
  for (const def of defs) {
    onPage = def.route ?? onPage;
    if (def.role !== undefined && onPage === "/sql") context.role = def.role;
    if (def.as_of !== undefined) context.asOf = def.as_of;
    if (def.as_recorded !== undefined) context.asRecorded = def.as_recorded;
    if (def.sql !== undefined) ran.set(def.key, { sql: def.sql, ...context });
    const step = demoTourStep(def, examples, notebook, steps[steps.length - 1], onPage, ran);
    steps.push(def.demo !== undefined ? { ...step, demo: def.demo } : step);
  }
  return steps;
}

/** A tour statement as the SQL explorer ran it: its text, run-as role and time travel. */
export interface StatementRun {
  sql: string;
  role?: string;
  asOf?: string;
  asRecorded?: string;
}

function demoTourStep(
  def: DemoTourStepDef,
  examples: readonly ExampleQuery[],
  notebook: readonly string[],
  before: TourStep | undefined,
  onPage: string | undefined,
  ran: ReadonlyMap<string, StatementRun>,
): TourStep {
  const page = {
    key: def.key,
    ...(def.route !== undefined ? { route: def.route, capability: def.capability } : {}),
  };
  if (def.source !== undefined) {
    return { ...page, element: `[data-testid="sources-row-${def.source}"]` };
  }
  if (def.table !== undefined && def.open !== undefined) {
    // The table's detail panel, open on the section (and Columns tab) the step is about: what it
    // describes is shown only there.
    const [section, tab] = def.open.split(".");
    const query = new URLSearchParams({ open: def.table, section, ...(tab ? { tab } : {}) });
    return {
      ...page,
      route: `${def.route ?? "/tables"}?${query.toString()}`,
      element: `[data-testid="table-section-${section}-panel"]`,
    };
  }
  if (def.table !== undefined) {
    return { ...page, element: `[data-table-row="${def.table}"]` };
  }
  if (def.relationship !== undefined) {
    return { ...page, element: `[data-tour-relationship="${def.relationship}"]` };
  }
  if (def.example !== undefined) {
    const example = examples.find((e) => e.name === def.example);
    // The server sends the demo's tour and its examples together; a step naming an example the
    // demo does not ship is a defect of the demo, which tests/unit/test_named_demo_tour.py holds.
    if (example === undefined) {
      throw new Error(`Tour: step ${def.key} names example "${def.example}", not shipped`);
    }
    return {
      ...page,
      openQuery: { stateKey: "query", text: example.query },
      element: ".graphiql-response",
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.sql !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "sql", text: def.sql },
      // Its rows, not the table: the grid of an unrun tab is a table that says it has none.
      readySelector: SQL_RESULT_ROW,
      element: ".sql-results-table",
      waitMs: RESULT_WAIT_MS,
    };
  }
  // Each runs on its page and is awaited as the SQL statement is: by a mark the page shows only
  // once this hand-off's answer is in.
  if (def.api !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "openApiUrl", text: def.api },
      readySelector: ".openapi-page[data-answered]",
      element: ".openapi-frame",
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.jsonapi !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "jsonapiUrl", text: def.jsonapi },
      readySelector: '[data-testid="jsonapi-summary-tab"]',
      element: ".jsonapi-panel",
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.grpc !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "grpcMethod", text: def.grpc },
      readySelector: ".grpc-panel-right[data-answered]",
      element: ".grpc-panel-right",
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.cypher !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "query", text: def.cypher },
      readySelector: '.graph-stream[data-handoff-status="done"]',
      element: ".graph-stream .gf-frame",
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.notebook === true) {
    // REQ-2084: the demo's notebook, its cells added to the Notebook tab's open notebook and run;
    // the step is shown once the last has answered.
    if (notebook.length === 0) {
      throw new Error(`Tour: step ${def.key} opens the demo's notebook, and it ships none`);
    }
    return {
      ...page,
      navState: { cells: [...notebook], autoRun: true },
      readySelector: '[data-testid="notebook-answered"]',
      element: '[data-testid="notebook-panel"]',
      waitMs: NOTEBOOK_WAIT_MS,
    };
  }
  if (def.group_by !== undefined) {
    return {
      ...page,
      navState: { view: { groupBy: def.group_by } },
      element: ".graph-stream .gf-frame",
    };
  }
  if (def.lineage !== undefined) {
    // The page builds a statement's graph from its deep link, read when the page opens.
    return {
      ...page,
      route: `${def.route ?? ""}?sql=${encodeURIComponent(def.lineage)}`,
      element: '[data-testid="lineage-graph"]',
      waitMs: RESULT_WAIT_MS,
    };
  }
  if (def.term !== undefined) {
    return {
      ...page,
      clickBefore: `[data-glossary-term="${def.term}"]`,
      element: '[data-testid="glossary-detail"]',
    };
  }
  if (def.product !== undefined) {
    // The page opens the product its deep link names (`?product=<id>`), read when it opens.
    return {
      ...page,
      route: `${def.route ?? ""}?product=${encodeURIComponent(def.product)}`,
      element: '[data-testid="data-product-detail"]',
    };
  }
  if (def.role !== undefined && def.role.includes(",")) {
    // A set of roles, a derived role among them (reviewer,region__eu, REQ-2057): handed to the
    // SQL explorer, which offers it as its own run-as entry and picks it.
    return {
      ...page,
      openQuery: { stateKey: "role", text: def.role },
      element: RUN_AS,
    };
  }
  if (def.role !== undefined && onPage !== "/sql") {
    // Anywhere but the SQL explorer, the role every request of the app runs as: the header's.
    return {
      ...page,
      element: APP_ROLE,
      chooseOnNext: { open: APP_ROLE, option: `[data-role-option="${def.role}"]` },
    };
  }
  if (def.role !== undefined) {
    return {
      ...page,
      element: RUN_AS,
      chooseOnNext: { open: RUN_AS, option: `[data-sql-role-option="${def.role}"]` },
    };
  }
  // Each handed to the SQL explorer, which sets its picker; shown on the picker.
  if (def.as_of !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "asOf", text: def.as_of },
      element: '[data-testid="sql-as-of"]',
    };
  }
  if (def.as_recorded !== undefined) {
    return {
      ...page,
      openQuery: { stateKey: "asRecorded", text: def.as_recorded },
      element: '[data-testid="sql-as-recorded"]',
    };
  }
  if (def.as_recorded_from !== undefined) {
    const { step, row, column } = def.as_recorded_from;
    const source = ran.get(step);
    // Only a `sql` step before this one has an answer to take the time from; the tour test holds it.
    if (source === undefined) {
      throw new Error(`Tour: step ${def.key} takes its time from "${step}", no sql step before it`);
    }
    return {
      ...page,
      openQuery: { stateKey: "asRecorded", text: "", from: { ...source, row, column } },
      element: '[data-testid="sql-as-recorded"]',
    };
  }
  if (def.env !== undefined) {
    return {
      ...page,
      element: ENV_SWITCHER,
      chooseOnNext: { open: ENV_SWITCHER, option: `[data-env-option="${def.env}"]` },
    };
  }
  if (def.polly !== undefined) {
    if (def.answer === undefined) {
      throw new Error(`Tour: step ${def.key} asks Polly with no stored answer`);
    }
    return {
      ...page,
      pollyOpen: true,
      pollyExchange: { question: def.polly, answer: def.answer },
      element: '[data-testid="chat-panel"]',
    };
  }
  if (def.image !== undefined) {
    // On the page of the step before, beside what that step showed: a picture of what the demo
    // looks like outside Provisa (an app, a BI tool) that reads it.
    if (before === undefined) {
      throw new Error(`Tour: step ${def.key} shows image "${def.image}" with no step before it`);
    }
    // The server sends the picture with the step (core/demo.py demo_tour).
    if (def.image_src === undefined) {
      throw new Error(`Tour: step ${def.key} shows image "${def.image}" the server did not send`);
    }
    return { ...page, element: before.element, image: def.image_src };
  }
  throw new Error(`Tour: step ${def.key} names no target`);
}

/**
 * REQ-2083: the value an `as_recorded_from` step takes, read when it runs: the earlier statement
 * run again as it ran (its role, else the role the app acts as; its time travel), and row
 * `row`'s `column`. A refused statement, or an answer without that row or column, fails the step
 * by name: there is no time to stand in for it.
 */
export async function answerValue(
  from: NonNullable<NonNullable<TourStep["openQuery"]>["from"]>,
  actingRoles: string | null,
): Promise<string> {
  const role = from.role ?? actingRoles;
  if (role === null) throw new Error("Tour: no role to run the earlier statement as");
  const answer = await runSql(from.sql, role, false, {
    asOf: from.asOf ?? null,
    asRecorded: from.asRecorded ?? null,
  });
  if (answer.error) throw new Error(`Tour: the earlier statement was refused: ${answer.error}`);
  const row = answer.rows[from.row];
  if (row === undefined || !(from.column in row) || row[from.column] === null) {
    throw new Error(`Tour: the earlier statement has no ${from.column} in row ${from.row}`);
  }
  return String(row[from.column]);
}
