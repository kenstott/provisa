// Copyright (c) 2026 Kenneth Stott
// Canary: d7542b15-c423-47dd-9992-d67dc8ddedca
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/* eslint-disable react-refresh/only-export-components -- context Provider + hook + storage helper colocated by design */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { exportProviderName } from "../lib/exportProviders";
import { driver, type DriveStep, type Driver } from "driver.js";
import "driver.js/dist/driver.css";
import "./tour.css";
import {
  TOUR_STEPS,
  TOUR_SCOPES,
  TOUR_PREREQUISITES,
  TOPIC_IDS,
  stepRoute,
  tourItinerary,
  stepsTheOrgCanShow,
  PET_STORE_DEMO_SQL,
  LINEAGE_DEMO_SQL,
  type RunScope,
  type TopicId,
  type TourStep,
} from "./tourSteps";
import {
  answerValue,
  demoStepShown,
  demoTourPrerequisites,
  demoTourSteps,
  demoTrackStamp,
} from "./demoTour";
import { prerequisitesMet, type OrgHas } from "./tourPrerequisites";
import { useNamedDemo } from "../context/NamedDemoContext";
import { TourMenu } from "./TourMenu";
import { completedTopics, markTopicCompleted } from "./tourTopics";
import { EXPANDED_STORAGE_KEY as DATA_PRODUCTS_EXPANDED_KEY } from "../pages/DataProductsPage";
import { useAuth } from "../context/AuthContext";
import { usePolly } from "../context/pollyState";
import { hasCapability } from "../lib/capabilities";
import { prefetchAllPageChunks } from "../pageChunks";
import { useTourPrefetch } from "../hooks/useAdminQueries";
import { useOrgHas } from "../hooks/useOrgHas";
import { prefetchSettings } from "../api/admin";
import { prefetchLineageGraph } from "../api/lineage";
import {
  TOUR_SEEN_KEY,
  TOUR_DEMO_RESET_KEY,
  TOUR_DECLINED_KEY,
  TOUR_OFFERED_KEY,
  TOUR_TOPICS_DONE_KEY,
} from "./tourKeys";
import { getStored, removeStored, setStored } from "../lib/browserStorage";
import { runUiActions } from "../mcp/uiActions";

/** REQ-2010: the named demo's own track was walked to its end in this browser. Saved per control
 * plane and org (REQ-2008), so each demo, and each fresh start of one, offers its track first. It
 * holds the track's stamp (demoTrackStamp): a revised track is not done until walked again. */
const DEMO_TRACK_DONE_KEY = "provisa_tour_demo_track_done";

function demoTrackDone(stamp: string): boolean {
  return getStored(DEMO_TRACK_DONE_KEY) === stamp;
}

/** Two frames: long enough for React to commit what a navigation hand-off changed. */
function nextPaint(): Promise<void> {
  return new Promise((resolve) =>
    requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
  );
}

// How long a step waits for its anchor to stop moving (a smooth scroll bringing it into view).
const SETTLE_MS = 1000;

/** Resolves once ``element`` holds still for a few frames, or after SETTLE_MS: its popover is
 * placed against where it stops, not where a smooth scroll found it (View Profile Runs' graph,
 * scrolled into view inside the window, had its popover drawn over it). */
function stopped(element: HTMLElement): Promise<void> {
  const until = performance.now() + SETTLE_MS;
  const at = () => {
    const r = element.getBoundingClientRect();
    return `${r.left},${r.top},${r.width},${r.height}`;
  };
  return new Promise((resolve) => {
    let last = at();
    let still = 0;
    const tick = () => {
      const now = at();
      still = now === last ? still + 1 : 0;
      last = now;
      if (still >= 3 || performance.now() > until) resolve();
      else requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
}

// localStorage keys owned by NlPage — mirrored here so the tour can seed a
// canned result (NL needs an external LLM key, which a demo visitor lacks).
const NL_QUESTION_KEY = "nl-question";
const NL_BRANCHES_KEY = "nl-branches";
// Backup of the visitor's own NL state while the tour's canned data is shown;
// restored verbatim on finish so the tour never clobbers real work.
const NL_BACKUP_KEY = "provisa_tour_nl_backup";

/**
 * Canned "how many pets per species" result — the six compiled query forms the
 * NL page renders. Matches the demo dataset so the panels look live without an
 * LLM call. `null` result = query-only branch (no inline table).
 */
const NL_DEMO_QUESTION = "how many pets per species";
const NL_DEMO_BRANCHES = {
  sql: {
    query: PET_STORE_DEMO_SQL,
    result: {
      columns: ["species", "pet_count"],
      rows: [
        { species: "rabbit", pet_count: 1 },
        { species: "cat", pet_count: 2 },
        { species: "dog", pet_count: 1 },
        { species: "lion", pet_count: 3 },
      ],
    },
    error: null,
    loading: false,
  },
  graphql: {
    query:
      "query PetsCountPerSpecies {\n" +
      "  ps__petsGroupBy(by: [species]) {\n" +
      "    groupKey\n" +
      "    aggregate {\n" +
      "      count\n" +
      "    }\n" +
      "  }\n" +
      "}",
    result: {
      data: {
        ps__petsGroupBy: [
          { groupKey: { species: "rabbit" }, aggregate: { count: 1 } },
          { groupKey: { species: "cat" }, aggregate: { count: 2 } },
          { groupKey: { species: "dog" }, aggregate: { count: 1 } },
          { groupKey: { species: "lion" }, aggregate: { count: 3 } },
        ],
      },
    },
    error: null,
    loading: false,
  },
  cypher: {
    query:
      "MATCH (p:Pets)\n" +
      "WITH p.species AS species, count(p) AS petCount\n" +
      "RETURN species, petCount\n" +
      "ORDER BY petCount DESC",
    result: {
      columns: ["species", "petCount"],
      rows: [
        { species: "lion", petCount: 3 },
        { species: "cat", petCount: 2 },
        { species: "dog", petCount: 1 },
        { species: "rabbit", petCount: 1 },
      ],
    },
    error: null,
    loading: false,
  },
  grpc: {
    query: "QueryPsPetsGroupBy(by=[species], funcs=[count])",
    result: null,
    error: null,
    loading: false,
  },
  jsonapi: {
    query: "/data/jsonapi/pet-store/pets?groupBy=species&aggregate=count",
    result: null,
    error: null,
    loading: false,
  },
  openapi: {
    query: "GET /data/rest/pet-store/pets?groupBy=species&aggregate=count",
    result: null,
    error: null,
    loading: false,
  },
};

/**
 * Prep actions run before a step navigates. Each seeds transient demo state and
 * is undone by {@link cleanupPrep} when the tour ends.
 */
// Canned MCP chat shown during the tour — a real chat needs the visitor's own LLM key, so the
// tour pre-seeds a representative exchange instead. McpExplorePage prefers this key when present.
const MCP_TOUR_KEY = "provisa_mcp_tour_chat";
const MCP_TOUR_CHAT = [
  { role: "user", text: "Which tables have inquiries, and how many are there?" },
  {
    role: "assistant",
    text:
      "The `pet_store.inquiries` table holds customer inquiries. Running a governed count:\n\n" +
      "| metric | value |\n|---|---|\n| total inquiries | 128 |\n| open | 34 |\n| resolved | 94 |\n\n" +
      "Every row is filtered by your role's domain access — I only ran `SELECT count(*)` through " +
      "the same governed pipeline pgwire and the other surfaces use.",
  },
];

const PREP_ACTIONS: Record<string, () => void> = {
  collapseDataProducts() {
    // The Data Products step clicks the first row to EXPAND it; a persisted expansion from an
    // earlier visit would make that same click collapse it instead. Nothing to restore: which
    // row was open is a convenience, not state the visitor authored.
    try {
      removeStored(DATA_PRODUCTS_EXPANDED_KEY);
    } catch {
      /* localStorage unavailable -- the page mounts collapsed anyway */
    }
  },
  seedMcp() {
    try {
      setStored(MCP_TOUR_KEY, JSON.stringify(MCP_TOUR_CHAT), { session: true });
    } catch {
      /* best-effort — the tour bubble text already describes the surface */
    }
  },
  seedNl() {
    // Snapshot the visitor's NL state once (guard against re-entry via Back).
    if (getStored(NL_BACKUP_KEY) === null) {
      setStored(
        NL_BACKUP_KEY,
        JSON.stringify({
          question: getStored(NL_QUESTION_KEY),
          branches: getStored(NL_BRANCHES_KEY),
        }),
      );
    }
    setStored(NL_QUESTION_KEY, NL_DEMO_QUESTION);
    setStored(NL_BRANCHES_KEY, JSON.stringify(NL_DEMO_BRANCHES));
  },
};

// Maps a tour step's `openBranch` to the explorer route's location-state key and the
// canned query to inject — mirrors NlPage's EXPLORER_ROUTES / openInExplorer so the
// tour lands on the explorer with the demo query pre-filled and auto-run.
const BRANCH_NAV: Record<
  NonNullable<TourStep["openBranch"]>,
  { stateKey: string; query: string }
> = {
  sql: { stateKey: "sql", query: NL_DEMO_BRANCHES.sql.query },
  graphql: { stateKey: "query", query: NL_DEMO_BRANCHES.graphql.query },
  cypher: { stateKey: "query", query: NL_DEMO_BRANCHES.cypher.query },
  grpc: { stateKey: "grpcMethod", query: NL_DEMO_BRANCHES.grpc.query },
  jsonapi: { stateKey: "jsonapiUrl", query: NL_DEMO_BRANCHES.jsonapi.query },
  openapi: { stateKey: "openApiUrl", query: NL_DEMO_BRANCHES.openapi.query },
};

/** Restore any state a prep action stashed. No-op if nothing was seeded. */
function cleanupPrep(): void {
  try {
    removeStored(MCP_TOUR_KEY, { session: true }); // drop the canned MCP chat when the tour ends
  } catch {
    /* ignore */
  }
  const raw = getStored(NL_BACKUP_KEY);
  if (raw === null) return;
  const restore = (key: string, value: string | null) =>
    value === null ? removeStored(key) : setStored(key, value);
  const { question, branches } = JSON.parse(raw) as {
    question: string | null;
    branches: string | null;
  };
  restore(NL_QUESTION_KEY, question);
  restore(NL_BRANCHES_KEY, branches);
  removeStored(NL_BACKUP_KEY);
}

// Step index the tour was on when dismissed early (X / Esc / backdrop). Lets the next launch pick
// up where the user left off. Cleared on completion (Done on the last step).
const TOUR_PROGRESS_KEY = "provisa_tour_progress";

// Marks a browser session as already reset. sessionStorage, not localStorage: "once per demo
// session" is exactly a tab's lifetime, and a new visitor on a kiosk browser gets a fresh one.
const DEMO_RESET_KEY = TOUR_DEMO_RESET_KEY;

/**
 * Clear the tour's own persisted state once per browser session on a demo server.
 *
 * A demo deployment is walked by a new visitor each time, and the previous visitor's seen-flag and
 * half-finished progress index turn the auto-start off or drop the next visitor into the middle of
 * the tour. Only the tour's keys are cleared — the session's bearer (`provisa_token`) and the NL
 * backup a running tour may be holding are not the tour's to discard.
 */
export function resetTourStateForDemoSession(): void {
  if (getStored(DEMO_RESET_KEY, { global: true, session: true }) === "true") return;
  setStored(DEMO_RESET_KEY, "true", { global: true, session: true });
  // Hand back anything a tour interrupted by a reload still had stashed before dropping the keys.
  cleanupPrep();
  removeStored(TOUR_SEEN_KEY, { global: true });
  removeStored(TOUR_PROGRESS_KEY, { global: true });
  // The previous visitor's "never again" is not this visitor's.
  removeStored(TOUR_DECLINED_KEY, { global: true });
  removeStored(TOUR_TOPICS_DONE_KEY, { global: true });
  removeStored(DEMO_TRACK_DONE_KEY);
}

/** True once the guided tour has been completed or dismissed on this browser. */
export function hasSeenTour(): boolean {
  return getStored(TOUR_SEEN_KEY, { global: true }) === "true";
}

/** True once the welcome modal's "don't show this again" has been ticked on this browser. */
export function tourDeclined(): boolean {
  return getStored(TOUR_DECLINED_KEY, { global: true }) === "true";
}

/** Remember the welcome modal's checkbox, so the offer never returns on this device. */
export function declineTour(): void {
  setStored(TOUR_DECLINED_KEY, "true", { global: true });
}

/**
 * Whether to open the welcome modal now, marking the offer pending if so.
 *
 * One offer per browser session, and none once the tour has been taken or permanently declined.
 * Reading and marking together keeps a double mount (StrictMode, a route remount) from counting as
 * two offers. An offer stays "pending" until the user answers it, so a remount of the shell -- the
 * identity gate re-reads /auth/me and drops its children while it does, which acknowledging a join
 * notice triggers -- puts the unanswered offer back rather than spending it.
 */
export function claimTourOffer(): boolean {
  if (hasSeenTour() || tourDeclined()) return false;
  if (getStored(TOUR_OFFERED_KEY, { global: true, session: true }) === "answered") return false;
  setStored(TOUR_OFFERED_KEY, "pending", { global: true, session: true });
  return true;
}

/** The offer was answered -- started, deferred, or declined. It does not return this session. */
export function resolveTourOffer(): void {
  setStored(TOUR_OFFERED_KEY, "answered", { global: true, session: true });
}

function isCoreStep(index: number): boolean {
  return TOUR_SCOPES.core.includes(TOUR_STEPS[index].key);
}

/**
 * The saved mid-tour step to resume from, or null if none / out of range. Only
 * steps past the first resume — the opening step is a fresh start.
 */
export function tourResumeStep(): number | null {
  const raw = getStored(TOUR_PROGRESS_KEY, { global: true });
  if (raw === null) return null;
  const n = parseInt(raw, 10);
  // Only the core tour is resumable (REQ-1945): a topic that is left returns to the menu.
  return Number.isInteger(n) && n > 0 && n < TOUR_STEPS.length && isCoreStep(n) ? n : null;
}

/**
 * Warm-ups named by `TourStep.prefetch`. Each starts the one uncached backend call the step's
 * anchor waits on, so the request is in flight before the route mounts instead of after.
 *
 * The start-up prefetch covers route chunks and the shared GraphQL queries; these are the REST
 * reads outside that set — `/admin/settings`, which gates TablesPage's (and /views') loading
 * state, and the lineage analysis, which the DAG step cannot paint without.
 */
export const PREFETCH_ACTIONS: Record<string, () => void> = {
  settings: () => prefetchSettings(),
  lineageDemo: () => prefetchLineageGraph(LINEAGE_DEMO_SQL),
};

/**
 * Fire the warm-up declared by step `index`, if any. Called both on entering a step (so a resume
 * straight into it still beats the page's own request) and one step early from the preceding
 * popover; every warm-up is idempotent, so the second call is a no-op while the first is unconsumed.
 */
function warmStep(index: number): void {
  const name = TOUR_STEPS[index]?.prefetch;
  if (!name) return;
  const warm = PREFETCH_ACTIONS[name];
  if (!warm) throw new Error(`Tour: step ${index} names unknown prefetch "${name}"`);
  warm();
}

/**
 * What the tour is doing while no popover is on screen.
 *
 * Every one of these states used to be invisible: the launch click sat silent through the start-up
 * prefetch, a step waiting on a slow page looked identical to a finished one, and an anchor that
 * never arrived simply ended the tour. On a loaded machine those gaps are seconds to tens of
 * seconds long, which reads as a dead button. `null` = a popover is showing, or nothing is running.
 */
export type TourStatus =
  | { kind: "preparing"; resuming: boolean }
  | { kind: "waiting"; step: number }
  | { kind: "stuck"; step: number }
  // REQ-2115: the step's own actions running, under its caption, before its popover.
  | { kind: "acting"; step: number; key: string }
  | null;

interface TourContextValue {
  /**
   * The tour button. Resumes a core tour left part-way unless { restart: true } (which starts the
   * core tour from the top); once the core tour has been seen it opens the Deep Dives menu.
   */
  startTour: (opts?: { restart?: boolean }) => void;
  /** True once the core tour has been taken or left, so the button opens the Deep Dives menu. */
  coreSeen: boolean;
  running: boolean;
  /** True when an earlier session was dismissed mid-tour and can be resumed. */
  canResume: boolean;
  /** Non-null while the tour is working with no popover up — drives the overlay and the navbar spinner. */
  status: TourStatus;
  /**
   * False when this viewer's rights open none of the tour's pages, so there is no tour to offer.
   * The launcher and the welcome modal hide rather than present a button that ends immediately.
   */
  available: boolean;
}

const TourContext = createContext<TourContextValue | null>(null);

/**
 * Resolve when an element matching `selector` is present in the DOM. Rejects
 * after `timeoutMs` so a genuinely missing anchor surfaces as a real error
 * rather than hanging the tour. No silent fallback — a failed wait aborts.
 *
 * The window is generous because each step may navigate to a lazily-loaded
 * route whose chunk is compiled/fetched on first visit; a short cap would abort
 * the whole tour on a cold chunk (observed killing it at the NL step in dev).
 * {@link ANCHOR_WAIT_MS} is sized for a machine under heavy load, where a page's
 * own render and its data round-trip both stretch; the wait is now visible while
 * it runs and recoverable when it expires, so a long one costs the visitor
 * nothing but a spinner.
 */
const ANCHOR_WAIT_MS = 45000;
type DriverPopover = NonNullable<DriveStep["popover"]>;

// How long an advance may take before the visitor is told the tour is waiting. Short enough that a
// stall never reads as a dead click, long enough that a normal advance shows no spinner at all.
const WAITING_HINT_MS = 1200;

// REQ-2115: how long each of a step's actions lets itself be seen, and the body class under which
// they run: driver.js's overlay and the previous popover are hidden then (tour.css), so what the
// actions open (a modal, a form) is seen whole.
const ACTION_PACE_MS = 700;
const ACTING_CLASS = "tour-acting";

function waitForElement(
  selector: string,
  timeoutMs = ANCHOR_WAIT_MS,
  signal?: AbortSignal,
): Promise<HTMLElement> {
  const existing = document.querySelector<HTMLElement>(selector);
  if (existing) return Promise.resolve(existing);
  // An abort raised before this wait began never fires its event again: without this the observer
  // would run its whole window against a document the provider has already left.
  if (signal?.aborted) return Promise.reject(new Error(`Tour: wait for ${selector} abandoned`));
  return new Promise((resolve, reject) => {
    const stop = () => {
      observer.disconnect();
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };
    const observer = new MutationObserver(() => {
      const el = document.querySelector<HTMLElement>(selector);
      if (el) {
        stop();
        resolve(el);
      }
    });
    const timer = setTimeout(() => {
      stop();
      reject(new Error(`Tour: element not found within ${timeoutMs}ms: ${selector}`));
    }, timeoutMs);
    // Without this the observer outlives the step that started it — up to ANCHOR_WAIT_MS of
    // observing a document the provider has already left.
    const onAbort = () => {
      stop();
      reject(new Error(`Tour: wait for ${selector} abandoned`));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    // Attributes too: a page marks a hand-off answered on an element already there (REQ-2010).
    observer.observe(document.body, { childList: true, subtree: true, attributes: true });
  });
}

export function TourProvider({ children }: { children: ReactNode }) {
  const navigate = useNavigate();
  const { t } = useTranslation();
  // The active step index drives the tour. null = not running. A state value
  // (not a ref) is what makes the runner effect fire reliably: startTour sets
  // it to 0, and every Next/Back is just another setState — no dependence on an
  // effect having pre-assigned a callback ref (which caused auto-start to no-op,
  // since child effects run before the parent's).
  const [activeStep, setActiveStep] = useState<number | null>(null);
  // Bumped by Retry so the runner effect re-enters the same step index.
  const [attempt, setAttempt] = useState(0);
  const [status, setStatus] = useState<TourStatus>(null);
  // Which tour (the core tour or one Deep Dives topic) activeStep belongs to, and whether the
  // Deep Dives menu is open (REQ-1945).
  const [activeScope, setActiveScope] = useState<RunScope | null>(null);
  const [menuOpen, setMenuOpen] = useState(false);
  // REQ-2013: what the org had when the menu last opened, which decides the tours it offers.
  const [orgHas, setOrgHas] = useState<OrgHas | null>(null);
  const prefetchTourData = useTourPrefetch();
  const whatTheOrgHas = useOrgHas();
  const { capabilities, actingRoles } = useAuth();
  // REQ-2083: what a step that re-runs an earlier statement runs it as, read when the step runs.
  const actingRolesRef = useRef(actingRoles);
  useEffect(() => {
    actingRolesRef.current = actingRoles;
  }, [actingRoles]);
  // REQ-2010: the named demo's own steps follow TOUR_STEPS in the list the runner walks; its track
  // is those steps (scope "demo").
  const namedDemo = useNamedDemo();
  // REQ-2012: a step asking Polly is in the track only with its stored answer.
  const allSteps = useMemo(
    () => [
      ...TOUR_STEPS,
      ...demoTourSteps(
        namedDemo.tour.filter(demoStepShown),
        namedDemo.exampleQueries,
        namedDemo.notebook,
      ),
    ],
    [namedDemo],
  );
  const trackStamp =
    namedDemo.name === null ? null : demoTrackStamp(namedDemo.name, namedDemo.tour);
  const trackDone = trackStamp !== null && demoTrackDone(trackStamp);
  const allStepsRef = useRef(allSteps);
  // Where the demo's last card's "Look around on my own" leads; read by the runner's closure.
  // REQ-2101 (amended): the org files the demo ships, read by a step's actions as it runs; held in a
  // ref, as the landing is, so a step is not run again when the status is read.
  const orgFilesRef = useRef(namedDemo.orgFiles);
  const landingRef = useRef(namedDemo.landing);
  useEffect(() => {
    landingRef.current = namedDemo.landing;
    orgFilesRef.current = namedDemo.orgFiles;
  }, [namedDemo]);
  useEffect(() => {
    allStepsRef.current = allSteps;
  }, [allSteps]);
  // REQ-1945: a step that refers to Polly opens the panel through the launcher's own handler. Read through
  // a ref because the runner's closure predates the render that last changed its state.
  const polly = usePolly();
  const pollyRef = useRef(polly);
  useEffect(() => {
    pollyRef.current = polly;
  }, [polly]);
  // Whether the tour opened the panel, so it closes only what it opened: a Polly the viewer already
  // had open is theirs to close.
  const tourOpenedPollyRef = useRef(false);
  const closeTourPolly = useCallback(() => {
    if (!tourOpenedPollyRef.current) return;
    tourOpenedPollyRef.current = false;
    pollyRef.current.closePolly();
  }, []);
  // The steps this viewer may actually be shown, in order, as indices into TOUR_STEPS. Everything
  // that used to count against TOUR_STEPS.length — the numbering, what Next/Back move to, which
  // step is last — counts against this instead, so a tour with steps dropped is a shorter whole
  // tour rather than one that ends early or numbers past its end.
  const itineraries = useMemo(() => {
    const meets = (capability: Parameters<typeof hasCapability>[1]) =>
      hasCapability(capabilities, capability);
    const out = { core: tourItinerary(meets, "core") } as Record<RunScope, number[]>;
    for (const id of TOPIC_IDS) out[id] = tourItinerary(meets, id);
    out.demo = tourItinerary(meets, "demo", allSteps);
    return out;
  }, [capabilities, allSteps]);
  // The active tour's steps. Everything below that walked "the tour" walks this. A tour starts only
  // once what the org has is read (startTour sets orgHas before the scope), so a running tour always
  // walks the steps its org can show; with no read yet, no tour runs and nothing walks this.
  const itinerary = useMemo(() => {
    const rights = itineraries[activeScope ?? "core"];
    return orgHas === null ? rights : stepsTheOrgCanShow(rights, orgHas, allSteps);
  }, [itineraries, activeScope, orgHas, allSteps]);
  // Read inside handlers whose closure predates a capability refresh. Effects only: startTour reads
  // the memo itself, because a child's effect runs before its parent's — `?tour=1` (TourAutoStart,
  // a child of this provider) called startTour on the very render the rights landed, when this ref
  // still held the empty itinerary of the render before. startTour declines an empty one, and the
  // param is spent by then, so the tour never started at all.
  // REQ-2013: a tour is offered when this viewer may open some of its pages and the org has what
  // its steps show.
  const offered = useCallback(
    (scope: RunScope, has: OrgHas) =>
      stepsTheOrgCanShow(itineraries[scope], has, allSteps).length > 0 &&
      prerequisitesMet(
        scope === "demo" ? demoTourPrerequisites(namedDemo.tour) : TOUR_PREREQUISITES[scope],
        has,
      ),
    [itineraries, namedDemo, allSteps],
  );
  // The Deep Dives menu, opened once the org's prerequisites have been checked.
  const openMenu = useCallback(() => {
    void whatTheOrgHas().then((has) => {
      setOrgHas(has);
      setMenuOpen(true);
    });
  }, [whatTheOrgHas]);
  const itineraryRef = useRef(itinerary);
  // What the step counter counts: the itinerary the tour started with. A role step mid-tour
  // changes the viewer's rights and so the itinerary; counted against that, "5 of 13" would turn
  // into "2 of 8". A step the new rights add is counted in its place among them.
  const countedRef = useRef<number[]>([]);
  const scopeRef = useRef<RunScope | null>(null);
  useEffect(() => {
    itineraryRef.current = itinerary;
    scopeRef.current = activeScope;
  }, [itinerary, activeScope]);
  const driverRef = useRef<Driver | null>(null);
  const currentPathRef = useRef<string>("");
  // REQ-2010: each entry into a step is a visit (Next, Back, a jump), counted as the active step
  // changes; the visit and attempt whose statement was last handed to an explorer.
  const visitRef = useRef(0);
  const handedRef = useRef<string | null>(null);
  // The visit and attempt whose step's clickBefore was last clicked: once per visit, as a statement
  // is handed once, since the runner re-runs a step (a new `t` or `navigate`) over what it opened.
  const clickedRef = useRef<string | null>(null);
  // Mirrors activeStep for handlers (onDestroyed) whose closure predates the current step.
  const activeStepRef = useRef<number | null>(null);

  // End the tour. "completed" (Done on the last step) clears saved progress; "dismissed" (X / Esc,
  // and the Exit choice on a stuck step) saves the current step so the next launch resumes there.
  //
  // A step whose anchor never arrives no longer ends the tour at all — it raises the "stuck" status
  // and offers Retry / Skip / Exit. Ending on it used to discard the saved index too, so one slow
  // page cost the visitor their whole position and the next Resume silently restarted at step 0.
  //
  // REQ-1945: the core tour ends on the Deep Dives menu (Done, or its Deep Dives button, "deepdives"),
  // and so does every topic however it ends; only a dismissed core tour saves a position instead.
  const endTour = useCallback(
    // "landed": a named demo's track walked to its end, left with "Look around on my own" -- done,
    // and no menu: the visitor is taken to the demo's landing page instead -- or left for another
    // demo a step links.
    (how: "completed" | "dismissed" | "deepdives" | "landed") => {
      setStatus(null);
      setStored(TOUR_SEEN_KEY, "true", { global: true });
      const scope = scopeRef.current;
      if (how !== "dismissed") {
        removeStored(TOUR_PROGRESS_KEY, { global: true });
      } else if (scope === "core" && activeStepRef.current != null) {
        setStored(TOUR_PROGRESS_KEY, String(activeStepRef.current), { global: true });
      }
      if ((how === "completed" || how === "landed") && scope === "demo" && trackStamp !== null)
        setStored(DEMO_TRACK_DONE_KEY, trackStamp);
      else if (how === "completed" && scope && scope !== "core" && scope !== "demo")
        markTopicCompleted(scope);
      if (how !== "landed" && (how !== "dismissed" || scope !== "core")) openMenu();
      cleanupPrep();
      closeTourPolly();
      // Null the ref before destroy so onDestroyed treats this as an intentional end, not a dismissal.
      const inst = driverRef.current;
      driverRef.current = null;
      inst?.destroy();
      setActiveStep(null);
      setActiveScope(null);
    },
    [closeTourPolly, openMenu, trackStamp],
  );

  const clickIfPresent = (selector?: string) => {
    if (!selector) return;
    document.querySelector<HTMLElement>(selector)?.click();
  };

  // Declared before the runner below, so a visit is counted before the runner reads it.
  useEffect(() => {
    visitRef.current += 1;
  }, [activeStep]);

  // Render whichever step is active. Re-runs on every setActiveStep.
  useEffect(() => {
    if (activeStep === null) return;
    const i = activeStep;
    activeStepRef.current = i;
    let cancelled = false;
    // Aborts any anchor wait still pending when the step changes or the provider unmounts.
    const abort = new AbortController();
    // An advance that resolves promptly should not flash a spinner; one that doesn't must say so.
    // The attempt is settled the moment it reaches ANY end — the popover is up, the step was
    // snapped forward, or it failed — and a settled attempt must never be re-announced as waiting.
    // Without this flag a failure raised inside the first WAITING_HINT_MS (a prep action throwing,
    // say) set "stuck" and was then overwritten by this timer, leaving the visitor on a spinner
    // whose only button is Cancel: the anchor timeout that would have offered Retry/Skip/Exit had
    // already been and gone.
    let settled = false;
    const waitingTimer = setTimeout(() => {
      if (!cancelled && !settled) setStatus({ kind: "waiting", step: i });
    }, WAITING_HINT_MS);
    const settle = () => {
      settled = true;
      clearTimeout(waitingTimer);
    };
    (async () => {
      try {
        // Indices only ever advance within range (step 0 has no Back button and
        // the last step calls finish() instead of advancing), so TOUR_STEPS[i]
        // is always defined here; a bad index would throw into the catch below.
        const step: TourStep = allStepsRef.current[i];
        // A saved position from a session with different rights, or one recorded before a grant was
        // withdrawn. Entering it would render NotAuthorized and wait out the anchor window, so move
        // to the next step this viewer does get; past the end there is nothing left to show.
        if (!itineraryRef.current.includes(i)) {
          settle();
          const next = itineraryRef.current.find((n) => n > i);
          if (next === undefined) endTour("completed");
          else setActiveStep(next);
          return;
        }
        // Before navigating, so the destination page's own call adopts this request rather than
        // opening a second one. Already in flight when the preceding step warmed it.
        warmStep(i);
        if (step.prep) PREP_ACTIONS[step.prep]?.();
        // The step's own route, or the one it inherits from an earlier step (see stepRoute) — a
        // resume can enter at any index, including one that omits `route` because it continues on
        // its predecessor's page. Stepping forward the inherited route already equals
        // currentPathRef, so the equality guard makes this a no-op exactly as before.
        const route = stepRoute(i, allStepsRef.current);
        if (route && step.openQuery) {
          // REQ-2010: a demo step's own statement, handed to its explorer once per visit to the
          // step: a new tab that runs it. This effect also re-runs within a visit when one of its
          // other inputs changes, and a second hand-off would open the statement twice; Retry hands
          // it over again. The page then commits that tab before the step's anchor is looked for, so a
          // result still on screen from the tab before is never taken for this one's.
          currentPathRef.current = route;
          const entry = `${visitRef.current}:${attempt}`;
          if (handedRef.current !== entry) {
            handedRef.current = entry;
            // REQ-2083: a value of an earlier statement's answer, read now: what Provisa recorded
            // is known only once it has.
            const text = step.openQuery.from
              ? await answerValue(step.openQuery.from, actingRolesRef.current)
              : step.openQuery.text;
            if (cancelled) return;
            // A bound value goes to the explorer as the next statement's values ($1).
            const handed = step.openQuery.stateKey === "params" ? [text] : text;
            navigate(route, {
              state: { [step.openQuery.stateKey]: handed, autoRun: true },
            });
          }
          await nextPaint();
          if (cancelled) return;
        } else if (route && step.navState) {
          // The page's state with no statement (the Graph frame's view, REQ-2010; the demo
          // notebook's cells, REQ-2084): once per visit, as a statement is.
          currentPathRef.current = route;
          const entry = `${visitRef.current}:${attempt}`;
          if (handedRef.current !== entry) {
            handedRef.current = entry;
            navigate(route, { state: step.navState });
          }
          await nextPaint();
          if (cancelled) return;
        } else if (
          route &&
          // The page actually shown, as well as the one the tour last sent the visitor to: a
          // record of the last navigation goes stale when the page changes under the tour, and
          // a step that trusted it would wait on its own page's anchor from another page.
          (route !== currentPathRef.current ||
            route !== `${window.location.pathname}${window.location.search}`)
        ) {
          currentPathRef.current = route;
          if (step.openBranch) {
            // Land on the explorer with the branch's demo query pre-filled + auto-run,
            // exactly as NlPage's "Open in X" button does.
            const { stateKey, query } = BRANCH_NAV[step.openBranch];
            navigate(route, { state: { [stateKey]: query, autoRun: true } });
          } else {
            navigate(route);
          }
        }
        // REQ-1945: Polly open for a step that refers to Polly; closed again, if the tour opened it,
        // on the first step that does not. A panel that fails to open leaves the anchor missing, so
        // the step fails loudly through the stuck status below -- no fallback anchor.
        if (step.pollyOpen) {
          if (step.pollyExchange) {
            // REQ-2012: the question and its stored answer, shown; nothing is sent.
            if (!pollyRef.current.open) tourOpenedPollyRef.current = true;
            pollyRef.current.showExchange(step.pollyExchange);
          } else if (!pollyRef.current.open) {
            tourOpenedPollyRef.current = true;
            await pollyRef.current.openPolly();
            if (cancelled) return;
          }
        } else {
          closeTourPolly();
        }
        for (const open of step.ensureOpen ?? []) {
          if (document.querySelector(open.unlessPresent)) continue;
          const opener = await waitForElement(open.click, undefined, abort.signal);
          // A run superseded while it waited must not click: the run that replaced it checks the
          // same state itself, and two clicks on a toggle leave it where it started.
          if (cancelled) return;
          opener.click();
          // What the click opens is what `unlessPresent` names. The next click waits for it: an
          // Edit that swaps the read view for the editor leaves the read view's own section tabs
          // on screen for a moment, and the next click, taken then, lands on a tab that is going
          // away -- the editor then opens on its first section and the step's anchor never comes.
          await waitForElement(open.unlessPresent, undefined, abort.signal);
          if (cancelled) return;
        }
        // Once per visit: a step run again (a superseded run, or the runner re-run on a new `t` or
        // `navigate`) clicked again, and a toggle clicked twice ("+ Source" opens and closes the
        // type picker) shut what the first click had opened -- the step then waited out its anchor
        // window on a picker it had closed itself. Retry is a new attempt, and clicks again.
        const clickEntry = `${visitRef.current}:${attempt}`;
        if (step.clickBefore && clickedRef.current !== clickEntry) {
          const trigger = await waitForElement(step.clickBefore, undefined, abort.signal);
          if (cancelled) return;
          clickedRef.current = clickEntry;
          trigger.click();
        }
        // REQ-2115: act, then explain. A step entered again finds what it built and only explains.
        if (step.actions && !document.querySelector(step.element)) {
          // The route just taken re-renders the provider, which supersedes this run: what a step
          // does is done once, by the run that stays, so it waits a paint and acts only if current.
          await nextPaint();
          if (cancelled) return;
          if (!document.querySelector(step.element)) {
            settle();
            setStatus({ kind: "acting", step: i, key: step.key });
            document.body.classList.add(ACTING_CLASS);
            let acted;
            try {
              const actions =
                typeof step.actions === "function"
                  ? step.actions({ orgFiles: orgFilesRef.current })
                  : step.actions;
              acted = await runUiActions(actions, {
                // The visitor started this tour: what it does is theirs, and asks nothing more.
                confirm: async () => true,
                paceMs: ACTION_PACE_MS,
                waitMs: ANCHOR_WAIT_MS,
                signal: abort.signal,
              });
            } finally {
              document.body.classList.remove(ACTING_CLASS);
            }
            if (cancelled) return;
            if (!acted.success) throw new Error(`Tour: ${acted.steps[acted.steps.length - 1]}`);
          }
        }
        // Gate on the destination page's own content, not just `step.element` — that selector
        // is sometimes a persistent-shell anchor (navbar/subnav) that exists before the page has
        // finished loading its data, which would otherwise show the popover over a page still
        // stuck on its own "Loading…" state.
        if (step.readySelector) {
          await waitForElement(step.readySelector, step.waitMs, abort.signal);
        }
        const element = await waitForElement(step.element, step.waitMs, abort.signal);
        // The anchor is here; whatever the visitor was told to wait for is done. Settled before the
        // guard below, because leaving with the timer armed is what strands the spinner.
        settle();
        if (cancelled || !driverRef.current) return;
        setStatus(null);
        // Expand a native <select> into an inline list box so its options and
        // <optgroup> headers are visible (a dropdown can't be opened
        // programmatically). The form is torn down on leave, so no restore.
        if (step.expandSelect && element instanceof HTMLSelectElement) {
          element.size = Math.min(12, element.options.length);
        }
        element.scrollIntoView({ block: "center", behavior: "smooth" });
        await stopped(element);
        if (cancelled || !driverRef.current) return;

        // Position within the itinerary, not within TOUR_STEPS: a viewer whose rights drop a
        // chapter gets "4 of 22", and the step before the dropped one is the one Next skips over.
        const steps = itineraryRef.current;
        const pos = steps.indexOf(i);
        const counted = [...new Set([...countedRef.current, ...steps])].sort((a, b) => a - b);
        const isFirst = pos === 0;
        // A step whose Next picks a role (or environment) changes the rights the itinerary follows,
        // so what comes after it is not the current rights' to say: "back to every role" is not the
        // end because the analyst may open nothing after it. It goes on through the steps counted,
        // and the runner moves past any the picked rights still deny.
        const onward = step.chooseOnNext ? counted : steps;
        const isLast = onward.indexOf(i) === onward.length - 1;
        // REQ-2010: a named demo's track ends on a choice -- the Deep Dives menu, or the demo's own
        // landing page -- not on a Done that rolls into another tour.
        const landing = scopeRef.current === "demo" && isLast ? landingRef.current : null;
        const ends = landing !== null;
        // A provider the copy names is named as a user sees it: "(beta)" while unproven.
        const copy = { atlan: exportProviderName("atlan", "Atlan", t) };
        const popover: DriverPopover = {
          title: t(`tour.steps.${step.key}.title`),
          description: step.image
            ? `${t(`tour.steps.${step.key}.description`, copy)}<img class="tour-step-image" src="${step.image}" alt="">`
            : t(`tour.steps.${step.key}.description`, copy),
          ...(step.image ? { popoverClass: "tour-popover-image" } : {}),
          showButtons: ends
            ? isFirst
              ? ["close"]
              : ["previous", "close"]
            : isFirst
              ? ["next", "close"]
              : ["previous", "next", "close"],
          nextBtnText: isLast
            ? t("tour.nav.done")
            : t("tour.nav.next", { current: counted.indexOf(i) + 1, total: counted.length }),
          prevBtnText: t("tour.nav.back"),
          onNextClick: () => {
            clickIfPresent(step.clickAfterNext);
            const advance = () => {
              if (isLast) endTour("completed");
              else setActiveStep(onward[onward.indexOf(i) + 1]);
            };
            const choose = step.chooseOnNext;
            if (!choose) return advance();
            // The option is painted only once the picker is open; a picker that never offers it
            // is the stuck step's to report, like any anchor that never arrives.
            document.querySelector<HTMLElement>(choose.open)?.click();
            waitForElement(choose.option, undefined, abort.signal).then(
              (option) => {
                option.click();
                advance();
              },
              () => setStatus({ kind: "stuck", step: i }),
            );
          },
          onPrevClick: () => {
            clickIfPresent(step.clickAfterNext);
            setActiveStep(steps[pos - 1]);
          },
          onCloseClick: () => {
            clickIfPresent(step.clickAfterNext);
            endTour("dismissed");
          },
          // Inject a "Start" button that jumps back to the opening step, so a
          // visitor can restart the core tour from any popover. Omitted on step 0,
          // where it would be a no-op, and on a topic, whose card carries Deep Dives
          // instead: the way back to the core tour's start is the catalog's control.
          onPopoverRender: (popover) => {
            // An image step's popover is placed before its picture loads; once it has, its
            // height is known and the popover is placed again, inside the window.
            const image =
              popover.description.querySelector<HTMLImageElement>("img.tour-step-image");
            if (image && !image.complete) {
              image.addEventListener("load", () => driverRef.current?.refresh(), { once: true });
            }
            // A named demo's step linking another demo: a button that leaves the tour for it.
            if (step.demo !== undefined) {
              const demo = step.demo;
              const open = document.createElement("button");
              open.type = "button";
              open.className = "driver-popover-demo-btn";
              open.dataset.testid = "tour-open-demo";
              open.textContent = t("tour.nav.openDemo", {
                title: t(`tour.demos.${demo}.title`),
              });
              open.addEventListener("click", () => {
                clickIfPresent(step.clickAfterNext);
                endTour("landed");
                navigate(`/demos/${demo}`);
              });
              popover.description.after(open);
            }
            if (!isFirst && scopeRef.current === "core") {
              const startBtn = document.createElement("button");
              startBtn.type = "button";
              startBtn.className = "driver-popover-start-btn";
              startBtn.textContent = t("tour.nav.start");
              startBtn.addEventListener("click", () => {
                clickIfPresent(step.clickAfterNext);
                setActiveStep(steps[0]);
              });
              popover.footerButtons.prepend(startBtn);
            }
            if (landing !== null) {
              const choice = document.createElement("div");
              choice.className = "tour-where-next";
              choice.dataset.testid = "tour-where-next";
              const heading = document.createElement("p");
              heading.className = "tour-where-next-heading";
              heading.textContent = t("tour.nav.whereNext");
              const buttons = document.createElement("div");
              buttons.className = "tour-where-next-buttons";
              const deep = document.createElement("button");
              deep.type = "button";
              deep.dataset.testid = "tour-where-next-deep-dives";
              deep.textContent = t("tour.nav.exploreDeepDives");
              deep.addEventListener("click", () => {
                clickIfPresent(step.clickAfterNext);
                endTour("completed"); // done, and the Deep Dives menu opens
              });
              const own = document.createElement("button");
              own.type = "button";
              own.dataset.testid = "tour-where-next-own";
              own.textContent = t("tour.nav.lookAround");
              own.addEventListener("click", () => {
                clickIfPresent(step.clickAfterNext);
                endTour("landed");
                navigate(landing.route);
              });
              buttons.append(deep, own);
              const note = document.createElement("p");
              note.className = "tour-where-next-note";
              note.textContent = t("tour.nav.anyTime");
              choice.append(heading, buttons, note);
              popover.footer.before(choice);
              return; // the choice is the card's way on: no Deep Dives button beside it
            }
            // REQ-1945: every card, core or topic, can leave for the Deep Dives menu at once.
            {
              const deepBtn = document.createElement("button");
              deepBtn.type = "button";
              deepBtn.className = "driver-popover-deepdives-btn";
              deepBtn.dataset.testid = "tour-deep-dives";
              deepBtn.textContent = t("tour.nav.deepDives");
              deepBtn.addEventListener("click", () => {
                clickIfPresent(step.clickAfterNext);
                endTour("deepdives");
              });
              popover.footerButtons.prepend(deepBtn);
            }
            // On the closing step of the last topic, offer a shortcut straight to the Docs tab.
            if (isLast && scopeRef.current === "operate") {
              const docsBtn = document.createElement("button");
              docsBtn.type = "button";
              docsBtn.className = "driver-popover-docs-btn";
              docsBtn.textContent = t("tour.nav.docs");
              docsBtn.addEventListener("click", () => {
                endTour("completed");
                navigate("/docs");
              });
              popover.footerButtons.prepend(docsBtn);
            }
          },
        };
        driverRef.current.highlight({ element, popover });
        // A page that renders its anchor again (the SQL explorer's results, once its answer is in)
        // leaves the step lighting a node no longer on the page, its popover in a corner: the
        // step's anchor as it is now is lit again.
        let lit = element;
        const relight = new MutationObserver(() => {
          if (lit.isConnected) return;
          const now = document.querySelector<HTMLElement>(step.element);
          if (!now || !driverRef.current) return;
          lit = now;
          driverRef.current.highlight({ element: now, popover });
        });
        relight.observe(document.body, { childList: true, subtree: true });
        abort.signal.addEventListener("abort", () => relight.disconnect(), { once: true });
        // The popover is on screen and the visitor is reading it — the idle window in which the
        // next step's backend work can run for free. Without this the request only starts when
        // Next is clicked, which is the delay that makes an advance feel dead on a loaded machine.
        // The NEXT step on this itinerary, which is not i + 1 when a step between them was dropped.
        if (!isLast) warmStep(steps[pos + 1]);
      } catch {
        settle();
        // The anchor never appeared: the page is still working, or this step's target is gone
        // (layout changed / gated by permission). The two are indistinguishable from here, so
        // hand the choice to the visitor — Retry re-enters the step, Skip moves past it, Exit
        // leaves with the position saved. The tour is not ended and progress is not discarded.
        if (cancelled) return;
        setStatus({ kind: "stuck", step: i });
      }
    })();
    return () => {
      cancelled = true;
      abort.abort();
      clearTimeout(waitingTimer);
    };
    // `attempt` is the Retry trigger: it re-runs this effect on the same step index.
  }, [activeStep, attempt, navigate, endTour, closeTourPolly, t]);

  // The driver paints its overlay and popover straight into document.body, outside React's tree, so
  // unmounting the provider does not remove them. Left behind they dim the page and keep a live
  // Next button whose handlers target a provider that no longer exists; the ref is nulled first so
  // onDestroyed does not read this as the visitor dismissing the tour.
  useEffect(
    () => () => {
      const inst = driverRef.current;
      driverRef.current = null;
      inst?.destroy();
    },
    [],
  );

  // Start the tour. Resumes from saved progress by default; pass { restart: true } to force step 0.
  //
  // The tour hops between surfaces faster than a step's own destination can pay its first-visit
  // route-chunk fetch/parse, so a step can end up highlighting a persistent-shell element (navbar/
  // subnav) while the destination page is still compiling underneath — the popover shows over a
  // page stuck on its own "Loading…" state. Awaiting every page chunk before the first step removes
  // that source of the race; the in-flight guard (`driverRef.current` set immediately) still blocks
  // a double-start from a second click during the wait.
  // Run one tour: the core tour (resuming a saved position unless `fresh`) or a Deep Dives topic.
  // REQ-2013: what a launch runs, picked once the org's prerequisites are known: a tour and whether
  // it starts fresh, or the Deep Dives menu, or nothing at all.
  type Pick = { scope: RunScope; fresh: boolean } | "menu" | null;
  const launch = useCallback(
    (pick: RunScope | ((has: OrgHas) => Pick), fresh: boolean) => {
      if (driverRef.current) return;
      // Nothing this viewer may open: every step's page is denied to them.
      if (typeof pick === "string" && itineraries[pick].length === 0) return;
      const resuming =
        (typeof pick === "string" ? pick === "core" : true) && !fresh && tourResumeStep() !== null;
      setMenuOpen(false);
      // The prefetch below is seconds of work on a loaded machine and the button gives no feedback
      // of its own; without this the click looks ignored and gets clicked again.
      setStatus({ kind: "preparing", resuming });
      currentPathRef.current = "";
      // The tour this launch runs, once it is picked.
      let running: RunScope | null = null;
      driverRef.current = driver({
        allowClose: true,
        overlayColor: "rgba(10, 12, 20, 0.7)",
        stagePadding: 6,
        stageRadius: 8,
        disableActiveInteraction: true,
        onDestroyed: () => {
          // Covers backdrop clicks / Esc, which bypass onCloseClick -- an early dismissal: the core
          // tour saves its step so the next launch resumes there, a topic returns to the menu.
          if (driverRef.current) {
            setStored(TOUR_SEEN_KEY, "true", { global: true });
            if (running === "core") {
              if (activeStepRef.current != null) {
                setStored(TOUR_PROGRESS_KEY, String(activeStepRef.current), { global: true });
              }
            } else {
              openMenu();
            }
            cleanupPrep();
            closeTourPolly();
            driverRef.current = null;
            setStatus(null);
            setActiveStep(null);
            setActiveScope(null);
          }
        },
      });
      // Both halves of "the destination is ready": its chunk is compiled and its queries are in the
      // cache. Waiting for the data too is what keeps a step from landing on a page still painting
      // its own "Loading..." state. REQ-2013: what runs is picked as soon as what the org has is
      // known; the Deep Dives menu opens then, a tour once its pages are ready too.
      const orgCheck = whatTheOrgHas();
      const ready = Promise.all([prefetchAllPageChunks(), prefetchTourData(), orgCheck]);
      void orgCheck
        .then((has) => {
          setOrgHas(has);
          // The wait may have outlasted a dismissal (e.g. immediate Esc) -- don't resurrect it.
          if (!driverRef.current) {
            setStatus(null);
            return;
          }
          const picked: Pick =
            typeof pick === "string"
              ? offered(pick, has)
                ? { scope: pick, fresh }
                : "menu"
              : pick(has);
          if (picked === null || picked === "menu") {
            // Nothing to run: the driver goes without counting as a dismissal.
            const inst = driverRef.current;
            driverRef.current = null;
            inst.destroy();
            setStatus(null);
            if (picked === "menu") setMenuOpen(true);
            return;
          }
          running = picked.scope;
          const steps = stepsTheOrgCanShow(itineraries[picked.scope], has, allStepsRef.current);
          void ready.then(() => {
            if (!driverRef.current) {
              setStatus(null);
              return;
            }
            // The step's own runner takes over the status from here: it clears it when the anchor
            // lands, or replaces it with waiting/stuck. A resumed index outside the itinerary is
            // snapped forward by the runner effect.
            setActiveScope(picked.scope);
            countedRef.current = steps;
            setActiveStep(
              picked.scope === "core" && !picked.fresh ? (tourResumeStep() ?? steps[0]) : steps[0],
            );
          });
        })
        .catch((e: unknown) => {
          // What the org has could not be read: no tour starts, and the launcher is not left
          // "preparing" for one that never will. The failure itself is not swallowed.
          const inst = driverRef.current;
          driverRef.current = null;
          inst?.destroy();
          setStatus(null);
          throw e;
        });
    },
    [itineraries, prefetchTourData, closeTourPolly, openMenu, whatTheOrgHas, offered],
  );

  // REQ-1945: the tour button. A core tour left part-way resumes; once the core tour has been seen
  // the button opens the Deep Dives menu; a first launch (or { restart: true }) runs the core tour.
  // REQ-2013: each only when the org has what its steps show.
  const startTour = useCallback(
    (opts?: { restart?: boolean }) => {
      if (driverRef.current) return;
      launch((has): Pick => {
        const ok = (scope: RunScope) => offered(scope, has);
        if (!(ok("core") || ok("demo") || TOPIC_IDS.some(ok))) return null;
        // REQ-2010: starting the tour from the top, in a named demo, starts with the demo's story;
        // the product tour is the Deep Dives menu's "Start at the Beginning".
        if (opts?.restart) {
          if (ok("demo")) return { scope: "demo", fresh: true };
          return ok("core") ? { scope: "core", fresh: true } : "menu";
        }
        if (tourResumeStep() !== null && ok("core")) return { scope: "core", fresh: false };
        // REQ-2010: a named demo tells its own story first, until it has been walked to its end.
        if (ok("demo") && !trackDone) return { scope: "demo", fresh: true };
        if (hasSeenTour() || !ok("core")) return "menu";
        return { scope: "core", fresh: true };
      }, !!opts?.restart);
    },
    [launch, offered, trackDone],
  );

  const menuTopics =
    orgHas === null
      ? []
      : TOPIC_IDS.filter((id) => offered(id, orgHas)).map((id) => ({
          id,
          steps: stepsTheOrgCanShow(itineraries[id], orgHas, allSteps).length,
        }));

  return (
    <TourContext.Provider
      value={{
        startTour,
        coreSeen: hasSeenTour(),
        running: activeStep !== null,
        canResume: tourResumeStep() !== null,
        status,
        available:
          TOPIC_IDS.some((id) => itineraries[id].length > 0) ||
          itineraries.core.length > 0 ||
          itineraries.demo.length > 0,
      }}
    >
      {children}
      <TourMenu
        opened={menuOpen}
        topics={menuTopics}
        completed={completedTopics()}
        demo={
          namedDemo.name !== null && orgHas !== null && offered("demo", orgHas)
            ? {
                name: namedDemo.name,
                steps: stepsTheOrgCanShow(itineraries.demo, orgHas, allSteps).length,
                done: trackDone,
              }
            : null
        }
        onDemo={() => launch("demo", true)}
        onPick={(id: TopicId) => launch(id, true)}
        // REQ-2013: the product tour's own start, when the org has what it shows.
        onCore={orgHas !== null && offered("core", orgHas) ? () => launch("core", true) : null}
        onClose={() => setMenuOpen(false)}
      />
      <TourStatusOverlay
        status={status}
        onRetry={() => {
          setStatus(null);
          setAttempt((n) => n + 1);
        }}
        onSkip={() => {
          const i = activeStepRef.current;
          if (i === null) return;
          setStatus(null);
          // The next step on this viewer's itinerary — skipping a step never lands on one their
          // rights would deny.
          const next = itineraryRef.current.find((n) => n > i);
          if (next === undefined) endTour("completed");
          else setActiveStep(next);
        }}
        onExit={() => endTour("dismissed")}
      />
    </TourContext.Provider>
  );
}

/**
 * The tour's own progress surface, shown whenever the tour is working with no popover on screen.
 *
 * It sits above driver.js's backdrop so a visitor waiting on a slow page sees what is being waited
 * for instead of a dimmed, unresponsive screen; on a stuck step it is also the only way out that
 * keeps the saved position.
 */
function TourStatusOverlay({
  status,
  onRetry,
  onSkip,
  onExit,
}: {
  status: TourStatus;
  onRetry: () => void;
  onSkip: () => void;
  onExit: () => void;
}) {
  const { t } = useTranslation();
  if (!status) return null;
  if (status.kind === "acting") {
    // A caption, not a card: the screen is what the visitor watches while the step acts.
    return (
      <div className="tour-acting-caption" role="status" aria-live="polite">
        {t(`tour.steps.${status.key}.acting`)}
      </div>
    );
  }
  const stuck = status.kind === "stuck";
  const message =
    status.kind === "preparing"
      ? status.resuming
        ? t("tour.status.resuming")
        : t("tour.status.starting")
      : status.kind === "waiting"
        ? t("tour.status.waiting")
        : t("tour.status.stuck");
  return (
    <div className="tour-status-overlay" role="status" aria-live="polite">
      <div className="tour-status-card">
        {!stuck && <div className="tour-status-spinner" aria-hidden />}
        <p className="tour-status-message">{message}</p>
        {stuck ? (
          <div className="tour-status-actions">
            <button type="button" className="tour-status-btn primary" onClick={onRetry}>
              {t("tour.status.retry")}
            </button>
            <button type="button" className="tour-status-btn" onClick={onSkip}>
              {t("tour.status.skip")}
            </button>
            <button type="button" className="tour-status-btn" onClick={onExit}>
              {t("tour.status.exit")}
            </button>
          </div>
        ) : (
          <div className="tour-status-actions">
            {/* A step still waiting can be passed over too: its page may never paint what the
                step points at, and a step that waits on an answer waits a minute before it is
                called stuck. */}
            {status.kind === "waiting" && (
              <button type="button" className="tour-status-btn" onClick={onSkip}>
                {t("tour.status.skip")}
              </button>
            )}
            <button type="button" className="tour-status-btn" onClick={onExit}>
              {t("tour.status.cancel")}
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

export function useTour(): TourContextValue {
  const ctx = useContext(TourContext);
  if (!ctx) throw new Error("useTour must be used within a TourProvider");
  return ctx;
}
