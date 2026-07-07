// Copyright (c) 2026 Kenneth Stott
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
import { driver, type Driver } from "driver.js";
import "driver.js/dist/driver.css";
import { TOUR_STEPS, type TourStep } from "./tourSteps";

const TOUR_SEEN_KEY = "provisa_tour_seen";

// localStorage keys owned by NlPage — mirrored here so the tour can seed a
// canned result (NL needs an external LLM key, which a demo visitor lacks).
const NL_QUESTION_KEY = "nl-question";
const NL_BRANCHES_KEY = "nl-branches";
// Backup of the visitor's own NL state while the tour's canned data is shown;
// restored verbatim on finish so the tour never clobbers real work.
const NL_BACKUP_KEY = "provisa_tour_nl_backup";

/**
 * Canned "show inquiry count by user" result — the six compiled query forms the
 * NL page renders. Matches the demo dataset so the panels look live without an
 * LLM call. `null` result = query-only branch (no inline table).
 */
const NL_DEMO_QUESTION = "show inquiry count by user";
const NL_DEMO_BRANCHES = {
  sql: {
    query:
      'SELECT users.name, COUNT(inquiries.id) AS inquiry_count FROM "default"."users" ' +
      'JOIN "default"."inquiries" ON users.id = inquiries.user_id GROUP BY users.name ' +
      "ORDER BY inquiry_count DESC LIMIT 100",
    result: {
      columns: ["name", "inquiry_count"],
      rows: [
        { name: "Hank Patel", inquiry_count: 3 },
        { name: "Alice Nguyen", inquiry_count: 3 },
        { name: "Grace Chen", inquiry_count: 3 },
        { name: "Frank Lee", inquiry_count: 3 },
        { name: "David Kim", inquiry_count: 3 },
        { name: "Eva Brown", inquiry_count: 3 },
        { name: "Carol White", inquiry_count: 3 },
        { name: "Bob Martinez", inquiry_count: 3 },
        { name: "Jay Singh", inquiry_count: 2 },
      ],
    },
    error: null,
    loading: false,
  },
  graphql: {
    query:
      "query InquiryCountByUser {\n" +
      "  ps__inquiriesGroupBy(by: [userId]) {\n" +
      "    groupKey\n" +
      "    aggregate {\n" +
      "      count\n" +
      "    }\n" +
      "  }\n" +
      "}",
    result: {
      data: {
        ps__inquiriesGroupBy: [
          { groupKey: { userId: 2 }, aggregate: { count: 3 } },
          { groupKey: { userId: 4 }, aggregate: { count: 3 } },
          { groupKey: { userId: 6 }, aggregate: { count: 3 } },
        ],
      },
    },
    error: null,
    loading: false,
  },
  cypher: {
    query:
      "MATCH (u:Users)-[:SUBMITTED]->(i:Inquiries)\n" +
      "WITH u.id AS userId, u.name AS userName, COUNT(i) AS inquiryCount\n" +
      "RETURN userId, userName, inquiryCount\n" +
      "ORDER BY inquiryCount DESC",
    result: {
      columns: ["userId", "userName", "inquiryCount"],
      rows: [
        { userId: 4, userName: "David Kim", inquiryCount: 3 },
        { userId: 2, userName: "Bob Martinez", inquiryCount: 3 },
        { userId: 6, userName: "Frank Lee", inquiryCount: 3 },
        { userId: 5, userName: "Eva Brown", inquiryCount: 3 },
        { userId: 3, userName: "Carol White", inquiryCount: 3 },
        { userId: 8, userName: "Hank Patel", inquiryCount: 3 },
        { userId: 7, userName: "Grace Chen", inquiryCount: 3 },
        { userId: 1, userName: "Alice Nguyen", inquiryCount: 3 },
      ],
    },
    error: null,
    loading: false,
  },
  grpc: { query: "QueryInquiries", result: null, error: null, loading: false },
  jsonapi: {
    query: "/data/jsonapi/pet-store/inquiries?page[size]=20",
    result: null,
    error: null,
    loading: false,
  },
  openapi: {
    query: "GET /data/rest/pet-store/inquiries",
    result: null,
    error: null,
    loading: false,
  },
};

/**
 * Prep actions run before a step navigates. Each seeds transient demo state and
 * is undone by {@link cleanupPrep} when the tour ends.
 */
const PREP_ACTIONS: Record<string, () => void> = {
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

/** Restore any state a prep action stashed. No-op if nothing was seeded. */
function cleanupPrep(): void {
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

/** True once the guided tour has been completed or dismissed on this browser. */
export function hasSeenTour(): boolean {
  return localStorage.getItem(TOUR_SEEN_KEY) === "true";
}

interface TourContextValue {
  /** Launch the guided feature tour from the first step. */
  startTour: () => void;
  running: boolean;
}

const TourContext = createContext<TourContextValue | null>(null);

/**
 * Resolve when an element matching `selector` is present in the DOM. Rejects
 * after `timeoutMs` so a missing anchor surfaces as a real error rather than
 * hanging the tour. No silent fallback — a failed wait aborts the tour.
 */
function waitForElement(selector: string, timeoutMs = 5000): Promise<HTMLElement> {
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
  // The active step index drives the tour. null = not running. A state value
  // (not a ref) is what makes the runner effect fire reliably: startTour sets
  // it to 0, and every Next/Back is just another setState — no dependence on an
  // effect having pre-assigned a callback ref (which caused auto-start to no-op,
  // since child effects run before the parent's).
  const [activeStep, setActiveStep] = useState<number | null>(null);
  const driverRef = useRef<Driver | null>(null);
  const currentPathRef = useRef<string>("");

  const finish = useCallback(() => {
    localStorage.setItem(TOUR_SEEN_KEY, "true");
    cleanupPrep();
    driverRef.current?.destroy();
    driverRef.current = null;
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
    let cancelled = false;
    (async () => {
      try {
        // Indices only ever advance within range (step 0 has no Back button and
        // the last step calls finish() instead of advancing), so TOUR_STEPS[i]
        // is always defined here; a bad index would throw into the catch below.
        const step: TourStep = TOUR_STEPS[i];
        if (step.prep) PREP_ACTIONS[step.prep]?.();
        if (step.route && step.route !== currentPathRef.current) {
          currentPathRef.current = step.route;
          navigate(step.route);
        }
        if (step.clickBefore) {
          const trigger = await waitForElement(step.clickBefore);
          trigger.click();
        }
        const element = await waitForElement(step.element);
        if (cancelled || !driverRef.current) return;
        element.scrollIntoView({ block: "center", behavior: "smooth" });

        const isLast = i === TOUR_STEPS.length - 1;
        driverRef.current.highlight({
          element,
          popover: {
            title: step.title,
            description: step.description,
            showButtons: i === 0 ? ["next", "close"] : ["previous", "next", "close"],
            nextBtnText: isLast ? "Done" : `Next (${i + 1}/${TOUR_STEPS.length})`,
            prevBtnText: "Back",
            onNextClick: () => {
              clickIfPresent(step.clickAfterNext);
              if (isLast) finish();
              else setActiveStep(i + 1);
            },
            onPrevClick: () => {
              clickIfPresent(step.clickAfterNext);
              setActiveStep(i - 1);
            },
            onCloseClick: () => {
              clickIfPresent(step.clickAfterNext);
              finish();
            },
          },
        });
      } catch {
        // Anchor never appeared (layout changed / gated by permission) — end
        // gracefully rather than trap the user behind an overlay.
        finish();
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [activeStep, navigate, finish]);

  const startTour = useCallback(() => {
    if (driverRef.current) return;
    currentPathRef.current = "";
    driverRef.current = driver({
      allowClose: true,
      overlayColor: "rgba(10, 12, 20, 0.7)",
      stagePadding: 6,
      stageRadius: 8,
      disableActiveInteraction: true,
      onDestroyed: () => {
        // Covers backdrop clicks / Esc, which bypass onCloseClick.
        if (driverRef.current) {
          localStorage.setItem(TOUR_SEEN_KEY, "true");
          cleanupPrep();
          driverRef.current = null;
          setActiveStep(null);
        }
      },
    });
    setActiveStep(0);
  }, []);

  return (
    <TourContext.Provider value={{ startTour, running: activeStep !== null }}>
      {children}
    </TourContext.Provider>
  );
}

export function useTour(): TourContextValue {
  const ctx = useContext(TourContext);
  if (!ctx) throw new Error("useTour must be used within a TourProvider");
  return ctx;
}
