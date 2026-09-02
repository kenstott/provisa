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
import { driver, type Driver } from "driver.js";
import "driver.js/dist/driver.css";
import "./tour.css";
import { TOUR_STEPS, stepRoute, tourItinerary, LINEAGE_DEMO_SQL, type TourStep } from "./tourSteps";
import { useAuth } from "../context/AuthContext";
import { hasCapability } from "../lib/capabilities";
import { prefetchAllPageChunks } from "../pageChunks";
import { useTourPrefetch } from "../hooks/useAdminQueries";
import { prefetchSettings } from "../api/admin";
import { prefetchLineageGraph } from "../api/lineage";
import {
  TOUR_SEEN_KEY,
  TOUR_DEMO_RESET_KEY,
  TOUR_DECLINED_KEY,
  TOUR_OFFERED_KEY,
} from "./tourKeys";

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
    query:
      'SELECT species, COUNT(*) AS pet_count FROM "pet_store"."pets" GROUP BY species LIMIT 100',
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
  seedMcp() {
    try {
      sessionStorage.setItem(MCP_TOUR_KEY, JSON.stringify(MCP_TOUR_CHAT));
    } catch {
      /* best-effort — the tour bubble text already describes the surface */
    }
  },
  seedNl() {
    // Snapshot the visitor's NL state once (guard against re-entry via Back).
    if (localStorage.getItem(NL_BACKUP_KEY) === null) {
      localStorage.setItem(
        NL_BACKUP_KEY,
        JSON.stringify({
          question: localStorage.getItem(NL_QUESTION_KEY),
          branches: localStorage.getItem(NL_BRANCHES_KEY),
        }),
      );
    }
    localStorage.setItem(NL_QUESTION_KEY, NL_DEMO_QUESTION);
    localStorage.setItem(NL_BRANCHES_KEY, JSON.stringify(NL_DEMO_BRANCHES));
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
    sessionStorage.removeItem(MCP_TOUR_KEY); // drop the canned MCP chat when the tour ends
  } catch {
    /* ignore */
  }
  const raw = localStorage.getItem(NL_BACKUP_KEY);
  if (raw === null) return;
  const restore = (key: string, value: string | null) =>
    value === null ? localStorage.removeItem(key) : localStorage.setItem(key, value);
  const { question, branches } = JSON.parse(raw) as {
    question: string | null;
    branches: string | null;
  };
  restore(NL_QUESTION_KEY, question);
  restore(NL_BRANCHES_KEY, branches);
  localStorage.removeItem(NL_BACKUP_KEY);
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
  if (sessionStorage.getItem(DEMO_RESET_KEY) === "true") return;
  sessionStorage.setItem(DEMO_RESET_KEY, "true");
  // Hand back anything a tour interrupted by a reload still had stashed before dropping the keys.
  cleanupPrep();
  localStorage.removeItem(TOUR_SEEN_KEY);
  localStorage.removeItem(TOUR_PROGRESS_KEY);
  // The previous visitor's "never again" is not this visitor's.
  localStorage.removeItem(TOUR_DECLINED_KEY);
}

/** True once the guided tour has been completed or dismissed on this browser. */
export function hasSeenTour(): boolean {
  return localStorage.getItem(TOUR_SEEN_KEY) === "true";
}

/** True once the welcome modal's "don't show this again" has been ticked on this browser. */
export function tourDeclined(): boolean {
  return localStorage.getItem(TOUR_DECLINED_KEY) === "true";
}

/** Remember the welcome modal's checkbox, so the offer never returns on this device. */
export function declineTour(): void {
  localStorage.setItem(TOUR_DECLINED_KEY, "true");
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
  if (sessionStorage.getItem(TOUR_OFFERED_KEY) === "answered") return false;
  sessionStorage.setItem(TOUR_OFFERED_KEY, "pending");
  return true;
}

/** The offer was answered -- started, deferred, or declined. It does not return this session. */
export function resolveTourOffer(): void {
  sessionStorage.setItem(TOUR_OFFERED_KEY, "answered");
}

/**
 * The saved mid-tour step to resume from, or null if none / out of range. Only
 * steps past the first resume — the opening step is a fresh start.
 */
export function tourResumeStep(): number | null {
  const raw = localStorage.getItem(TOUR_PROGRESS_KEY);
  if (raw === null) return null;
  const n = parseInt(raw, 10);
  return Number.isInteger(n) && n > 0 && n < TOUR_STEPS.length ? n : null;
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
  | null;

interface TourContextValue {
  /** Launch the guided feature tour. Resumes from saved progress unless { restart: true }. */
  startTour: (opts?: { restart?: boolean }) => void;
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

// How long an advance may take before the visitor is told the tour is waiting. Short enough that a
// stall never reads as a dead click, long enough that a normal advance shows no spinner at all.
const WAITING_HINT_MS = 1200;

function waitForElement(
  selector: string,
  timeoutMs = ANCHOR_WAIT_MS,
  signal?: AbortSignal,
): Promise<HTMLElement> {
  const existing = document.querySelector<HTMLElement>(selector);
  if (existing) return Promise.resolve(existing);
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
    observer.observe(document.body, { childList: true, subtree: true });
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
  const prefetchTourData = useTourPrefetch();
  const { capabilities } = useAuth();
  // The steps this viewer may actually be shown, in order, as indices into TOUR_STEPS. Everything
  // that used to count against TOUR_STEPS.length — the numbering, what Next/Back move to, which
  // step is last — counts against this instead, so a tour with steps dropped is a shorter whole
  // tour rather than one that ends early or numbers past its end.
  const itinerary = useMemo(
    () => tourItinerary((capability) => hasCapability(capabilities, capability)),
    [capabilities],
  );
  // Read inside handlers whose closure predates a capability refresh.
  const itineraryRef = useRef(itinerary);
  useEffect(() => {
    itineraryRef.current = itinerary;
  }, [itinerary]);
  const driverRef = useRef<Driver | null>(null);
  const currentPathRef = useRef<string>("");
  // Mirrors activeStep for handlers (onDestroyed) whose closure predates the current step.
  const activeStepRef = useRef<number | null>(null);

  // End the tour. "completed" (Done on the last step) clears saved progress; "dismissed" (X / Esc,
  // and the Exit choice on a stuck step) saves the current step so the next launch resumes there.
  //
  // A step whose anchor never arrives no longer ends the tour at all — it raises the "stuck" status
  // and offers Retry / Skip / Exit. Ending on it used to discard the saved index too, so one slow
  // page cost the visitor their whole position and the next Resume silently restarted at step 0.
  const endTour = useCallback((how: "completed" | "dismissed") => {
    setStatus(null);
    localStorage.setItem(TOUR_SEEN_KEY, "true");
    if (how !== "dismissed") {
      localStorage.removeItem(TOUR_PROGRESS_KEY);
    } else if (activeStepRef.current != null) {
      localStorage.setItem(TOUR_PROGRESS_KEY, String(activeStepRef.current));
    }
    cleanupPrep();
    // Null the ref before destroy so onDestroyed treats this as an intentional end, not a dismissal.
    const inst = driverRef.current;
    driverRef.current = null;
    inst?.destroy();
    setActiveStep(null);
  }, []);

  const clickIfPresent = (selector?: string) => {
    if (!selector) return;
    document.querySelector<HTMLElement>(selector)?.click();
  };

  // Render whichever step is active. Re-runs on every setActiveStep.
  useEffect(() => {
    if (activeStep === null) return;
    const i = activeStep;
    activeStepRef.current = i;
    let cancelled = false;
    // Aborts any anchor wait still pending when the step changes or the provider unmounts.
    const abort = new AbortController();
    // An advance that resolves promptly should not flash a spinner; one that doesn't must say so.
    const waitingTimer = setTimeout(() => {
      if (!cancelled) setStatus({ kind: "waiting", step: i });
    }, WAITING_HINT_MS);
    (async () => {
      try {
        // Indices only ever advance within range (step 0 has no Back button and
        // the last step calls finish() instead of advancing), so TOUR_STEPS[i]
        // is always defined here; a bad index would throw into the catch below.
        const step: TourStep = TOUR_STEPS[i];
        // A saved position from a session with different rights, or one recorded before a grant was
        // withdrawn. Entering it would render NotAuthorized and wait out the anchor window, so move
        // to the next step this viewer does get; past the end there is nothing left to show.
        if (!itineraryRef.current.includes(i)) {
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
        const route = stepRoute(i);
        if (route && route !== currentPathRef.current) {
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
        if (step.clickBefore) {
          const trigger = await waitForElement(step.clickBefore, undefined, abort.signal);
          trigger.click();
        }
        // Gate on the destination page's own content, not just `step.element` — that selector
        // is sometimes a persistent-shell anchor (navbar/subnav) that exists before the page has
        // finished loading its data, which would otherwise show the popover over a page still
        // stuck on its own "Loading…" state.
        if (step.readySelector) {
          await waitForElement(step.readySelector, step.waitMs, abort.signal);
        }
        const element = await waitForElement(step.element, step.waitMs, abort.signal);
        if (cancelled || !driverRef.current) return;
        // The anchor is here; whatever the visitor was told to wait for is done.
        clearTimeout(waitingTimer);
        setStatus(null);
        // Expand a native <select> into an inline list box so its options and
        // <optgroup> headers are visible (a dropdown can't be opened
        // programmatically). The form is torn down on leave, so no restore.
        if (step.expandSelect && element instanceof HTMLSelectElement) {
          element.size = Math.min(12, element.options.length);
        }
        element.scrollIntoView({ block: "center", behavior: "smooth" });

        // Position within the itinerary, not within TOUR_STEPS: a viewer whose rights drop a
        // chapter gets "4 of 22", and the step before the dropped one is the one Next skips over.
        const steps = itineraryRef.current;
        const pos = steps.indexOf(i);
        const isFirst = pos === 0;
        const isLast = pos === steps.length - 1;
        driverRef.current.highlight({
          element,
          popover: {
            title: t(`tour.steps.${step.key}.title`),
            description: t(`tour.steps.${step.key}.description`),
            showButtons: isFirst ? ["next", "close"] : ["previous", "next", "close"],
            nextBtnText: isLast
              ? t("tour.nav.done")
              : t("tour.nav.next", { current: pos + 1, total: steps.length }),
            prevBtnText: t("tour.nav.back"),
            onNextClick: () => {
              clickIfPresent(step.clickAfterNext);
              if (isLast) endTour("completed");
              else setActiveStep(steps[pos + 1]);
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
            // visitor can restart the tour from any popover. Omitted on step 0,
            // where it would be a no-op.
            onPopoverRender: (popover) => {
              if (!isFirst) {
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
              // On the closing step, offer a shortcut straight to the Docs tab.
              if (isLast) {
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
          },
        });
        // The popover is on screen and the visitor is reading it — the idle window in which the
        // next step's backend work can run for free. Without this the request only starts when
        // Next is clicked, which is the delay that makes an advance feel dead on a loaded machine.
        // The NEXT step on this itinerary, which is not i + 1 when a step between them was dropped.
        if (!isLast) warmStep(steps[pos + 1]);
      } catch {
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
  }, [activeStep, attempt, navigate, endTour, t]);

  // Start the tour. Resumes from saved progress by default; pass { restart: true } to force step 0.
  //
  // The tour hops between surfaces faster than a step's own destination can pay its first-visit
  // route-chunk fetch/parse, so a step can end up highlighting a persistent-shell element (navbar/
  // subnav) while the destination page is still compiling underneath — the popover shows over a
  // page stuck on its own "Loading…" state. Awaiting every page chunk before the first step removes
  // that source of the race; the in-flight guard (`driverRef.current` set immediately) still blocks
  // a double-start from a second click during the wait.
  const startTour = useCallback(
    (opts?: { restart?: boolean }) => {
      if (driverRef.current) return;
      // Nothing this viewer may open: every step's page is denied to them. There is no tour to
      // give, and the offer itself is withheld (see `tourAvailable`), so this is the belt to that
      // brace rather than a state anyone reaches by clicking.
      if (itineraryRef.current.length === 0) return;
      const resuming = !opts?.restart && tourResumeStep() !== null;
      // The prefetch below is seconds of work on a loaded machine and the button gives no feedback
      // of its own; without this the click looks ignored and gets clicked again.
      setStatus({ kind: "preparing", resuming });
      currentPathRef.current = "";
      driverRef.current = driver({
        allowClose: true,
        overlayColor: "rgba(10, 12, 20, 0.7)",
        stagePadding: 6,
        stageRadius: 8,
        disableActiveInteraction: true,
        onDestroyed: () => {
          // Covers backdrop clicks / Esc, which bypass onCloseClick — treat as an early
          // dismissal and save the current step so the next launch resumes there.
          if (driverRef.current) {
            localStorage.setItem(TOUR_SEEN_KEY, "true");
            if (activeStepRef.current != null) {
              localStorage.setItem(TOUR_PROGRESS_KEY, String(activeStepRef.current));
            }
            cleanupPrep();
            driverRef.current = null;
            setStatus(null);
            setActiveStep(null);
          }
        },
      });
      // Both halves of "the destination is ready": its chunk is compiled and its queries are in the
      // cache. Waiting for the data too is what keeps a step from landing on a page still painting
      // its own "Loading…" state.
      void Promise.all([prefetchAllPageChunks(), prefetchTourData()]).then(() => {
        // The wait may have outlasted a dismissal (e.g. immediate Esc) — don't resurrect it.
        if (!driverRef.current) {
          setStatus(null);
          return;
        }
        // The step's own runner takes over the status from here: it clears it when the anchor
        // lands, or replaces it with waiting/stuck.
        // The itinerary's own first step, which is TOUR_STEPS[0] only when this viewer gets it. A
        // resumed index outside the itinerary is snapped forward by the runner effect.
        setActiveStep(
          opts?.restart ? itineraryRef.current[0] : (tourResumeStep() ?? itineraryRef.current[0]),
        );
      });
    },
    [prefetchTourData],
  );

  return (
    <TourContext.Provider
      value={{
        startTour,
        running: activeStep !== null,
        canResume: tourResumeStep() !== null,
        status,
        available: itinerary.length > 0,
      }}
    >
      {children}
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
