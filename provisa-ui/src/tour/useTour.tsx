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
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useNavigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { driver, type Driver } from "driver.js";
import "driver.js/dist/driver.css";
import "./tour.css";
import { TOUR_STEPS, stepRoute, type TourStep } from "./tourSteps";
import { prefetchAllPageChunks } from "../pageChunks";
import { useTourPrefetch } from "../hooks/useAdminQueries";

const TOUR_SEEN_KEY = "provisa_tour_seen";

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

/** True once the guided tour has been completed or dismissed on this browser. */
export function hasSeenTour(): boolean {
  return localStorage.getItem(TOUR_SEEN_KEY) === "true";
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

interface TourContextValue {
  /** Launch the guided feature tour. Resumes from saved progress unless { restart: true }. */
  startTour: (opts?: { restart?: boolean }) => void;
  running: boolean;
  /** True when an earlier session was dismissed mid-tour and can be resumed. */
  canResume: boolean;
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
 */
function waitForElement(selector: string, timeoutMs = 15000): Promise<HTMLElement> {
  const existing = document.querySelector<HTMLElement>(selector);
  if (existing) return Promise.resolve(existing);
  return new Promise((resolve, reject) => {
    const observer = new MutationObserver(() => {
      const el = document.querySelector<HTMLElement>(selector);
      if (el) {
        observer.disconnect();
        clearTimeout(timer);
        resolve(el);
      }
    });
    const timer = setTimeout(() => {
      observer.disconnect();
      reject(new Error(`Tour: element not found within ${timeoutMs}ms: ${selector}`));
    }, timeoutMs);
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
  const prefetchTourData = useTourPrefetch();
  const driverRef = useRef<Driver | null>(null);
  const currentPathRef = useRef<string>("");
  // Mirrors activeStep for handlers (onDestroyed) whose closure predates the current step.
  const activeStepRef = useRef<number | null>(null);

  // End the tour. "completed" (Done on the last step) clears saved progress; "dismissed" (X / Esc)
  // saves the current step so the next launch resumes there. "failed" — the step's anchor never
  // appeared — also clears it: re-saving an index that could not render leaves the launch button
  // dead, since every later click would enter the same step and abort the same way.
  const endTour = useCallback((how: "completed" | "dismissed" | "failed") => {
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
    (async () => {
      try {
        // Indices only ever advance within range (step 0 has no Back button and
        // the last step calls finish() instead of advancing), so TOUR_STEPS[i]
        // is always defined here; a bad index would throw into the catch below.
        const step: TourStep = TOUR_STEPS[i];
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
          const trigger = await waitForElement(step.clickBefore);
          trigger.click();
        }
        // Gate on the destination page's own content, not just `step.element` — that selector
        // is sometimes a persistent-shell anchor (navbar/subnav) that exists before the page has
        // finished loading its data, which would otherwise show the popover over a page still
        // stuck on its own "Loading…" state.
        if (step.readySelector) {
          await waitForElement(step.readySelector, step.waitMs);
        }
        const element = await waitForElement(step.element, step.waitMs);
        if (cancelled || !driverRef.current) return;
        // Expand a native <select> into an inline list box so its options and
        // <optgroup> headers are visible (a dropdown can't be opened
        // programmatically). The form is torn down on leave, so no restore.
        if (step.expandSelect && element instanceof HTMLSelectElement) {
          element.size = Math.min(12, element.options.length);
        }
        element.scrollIntoView({ block: "center", behavior: "smooth" });

        const isLast = i === TOUR_STEPS.length - 1;
        driverRef.current.highlight({
          element,
          popover: {
            title: t(`tour.steps.${step.key}.title`),
            description: t(`tour.steps.${step.key}.description`),
            showButtons: i === 0 ? ["next", "close"] : ["previous", "next", "close"],
            nextBtnText: isLast
              ? t("tour.nav.done")
              : t("tour.nav.next", { current: i + 1, total: TOUR_STEPS.length }),
            prevBtnText: t("tour.nav.back"),
            onNextClick: () => {
              clickIfPresent(step.clickAfterNext);
              if (isLast) endTour("completed");
              else setActiveStep(i + 1);
            },
            onPrevClick: () => {
              clickIfPresent(step.clickAfterNext);
              setActiveStep(i - 1);
            },
            onCloseClick: () => {
              clickIfPresent(step.clickAfterNext);
              endTour("dismissed");
            },
            // Inject a "Start" button that jumps back to the opening step, so a
            // visitor can restart the tour from any popover. Omitted on step 0,
            // where it would be a no-op.
            onPopoverRender: (popover) => {
              if (i !== 0) {
                const startBtn = document.createElement("button");
                startBtn.type = "button";
                startBtn.className = "driver-popover-start-btn";
                startBtn.textContent = t("tour.nav.start");
                startBtn.addEventListener("click", () => {
                  clickIfPresent(step.clickAfterNext);
                  setActiveStep(0);
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
      } catch {
        // Anchor never appeared (layout changed / gated by permission) — end
        // gracefully, saving progress so the user can resume rather than being
        // trapped behind an overlay.
        endTour("failed");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [activeStep, navigate, endTour, t]);

  // Start the tour. Resumes from saved progress by default; pass { restart: true } to force step 0.
  //
  // The tour hops between surfaces faster than a step's own destination can pay its first-visit
  // route-chunk fetch/parse, so a step can end up highlighting a persistent-shell element (navbar/
  // subnav) while the destination page is still compiling underneath — the popover shows over a
  // page stuck on its own "Loading…" state. Awaiting every page chunk before the first step removes
  // that source of the race; the in-flight guard (`driverRef.current` set immediately) still blocks
  // a double-start from a second click during the wait.
  const startTour = useCallback((opts?: { restart?: boolean }) => {
    if (driverRef.current) return;
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
          setActiveStep(null);
        }
      },
    });
    // Both halves of "the destination is ready": its chunk is compiled and its queries are in the
    // cache. Waiting for the data too is what keeps a step from landing on a page still painting
    // its own "Loading…" state.
    void Promise.all([prefetchAllPageChunks(), prefetchTourData()]).then(() => {
      // The wait may have outlasted a dismissal (e.g. immediate Esc) — don't resurrect it.
      if (!driverRef.current) return;
      setActiveStep(opts?.restart ? 0 : (tourResumeStep() ?? 0));
    });
  }, [prefetchTourData]);

  return (
    <TourContext.Provider
      value={{ startTour, running: activeStep !== null, canResume: tourResumeStep() !== null }}
    >
      {children}
    </TourContext.Provider>
  );
}

export function useTour(): TourContextValue {
  const ctx = useContext(TourContext);
  if (!ctx) throw new Error("useTour must be used within a TourProvider");
  return ctx;
}
