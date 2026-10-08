// Copyright (c) 2026 Kenneth Stott
// Canary: 6d1a8f35-2b4e-4c70-9e83-d7f2a05b1c69
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1945: the tour is a core tour plus a menu of Deep Dives topics, each its own tour. Every step
 * belongs to exactly one scope, a scoped itinerary keeps TOUR_STEPS order, and completion marks are
 * per-viewer localStorage that tolerates a browser refusing storage.
 */

import { describe, it, expect, beforeEach, vi } from "vitest";
import { TOUR_STEPS, TOUR_SCOPES, TOPIC_IDS, tourItinerary, type TourScope } from "../tour/tourSteps";
import { completedTopics, markTopicCompleted } from "../tour/tourTopics";
import { TOUR_TOPICS_DONE_KEY } from "../tour/tourKeys";
import enTour from "../i18n/locales/en/tour.json";

const ALL: TourScope[] = ["core", ...TOPIC_IDS];
const everything = () => true;

describe("tour scopes", () => {
  it("assigns every step to exactly one scope, and every scoped key to a real step", () => {
    const owners = new Map<string, TourScope[]>();
    for (const scope of ALL) {
      for (const key of TOUR_SCOPES[scope]) owners.set(key, [...(owners.get(key) ?? []), scope]);
    }
    for (const step of TOUR_STEPS) {
      expect(owners.get(step.key), `step ${step.key}`).toHaveLength(1);
    }
    expect(owners.size).toBe(TOUR_STEPS.length);
  });

  it("makes the core tour welcome, Polly, connecting a source and querying it", () => {
    expect(TOUR_SCOPES.core.slice(0, 2)).toEqual(["step0", "stepPolly"]);
    expect(TOUR_SCOPES.core).toContain("step2"); // register a source
    expect(TOUR_SCOPES.core).toContain("stepQuery");
    expect(TOUR_SCOPES.core.filter((k) => k.startsWith("step") && TOUR_STEPS.find((s) => s.key === k)?.route?.match(/^\/(sql|query|graph|grpc|jsonapi|openapi|explore|nl)$/))).toEqual(["stepQuery"]);
  });

  it("puts every query surface's detail in the Query it everywhere topic", () => {
    expect(TOUR_SCOPES.query).toEqual(["step6", "step7", "step8", "step9", "step10", "step11", "step12", "step13"]);
  });

  it("lists the Deep Dives topics in menu order, Relationships right after Connect", () => {
    expect(TOPIC_IDS).toEqual([
      "connect",
      "relationships",
      "model",
      "govern",
      "quality",
      "query",
      "testdata",
      "publish",
      "operate",
    ]);
  });

  it("gives every Deep Dive topic at least four steps", () => {
    for (const id of TOPIC_IDS) expect(TOUR_SCOPES[id].length, id).toBeGreaterThanOrEqual(4);
  });

  it("ends no topic, and not the core tour, on a whole-tour sign-off", () => {
    for (const id of TOPIC_IDS) expect(TOUR_SCOPES[id].at(-1), id).not.toBe("step24");
    expect(TOUR_STEPS.map((s) => s.key)).not.toContain("step24");
    expect(TOUR_SCOPES.core.at(-1)).toBe("step14");
  });

  it("ends the Model topic on lineage", () => {
    expect(TOUR_SCOPES.model.at(-1)).toBe("step22");
  });

  it("gives every topic and the Deep Dives button their translated text", () => {
    const tour = enTour.tour as unknown as {
      nav: Record<string, string>;
      topics: Record<string, { title: string; summary: string }>;
      menu: Record<string, string>;
    };
    expect(tour.nav.deepDives).toBe("Deep Dives");
    expect(tour.menu.title).toBe("Deep Dives");
    for (const id of TOPIC_IDS) {
      expect(tour.topics[id].title, id).toBeTruthy();
      expect(tour.topics[id].summary, id).toBeTruthy();
    }
  });

  it("scopes an itinerary to its own steps, in TOUR_STEPS order, and partitions the whole tour", () => {
    const union: number[] = [];
    for (const scope of ALL) {
      const it = tourItinerary(everything, scope);
      expect(it.length, scope).toBe(TOUR_SCOPES[scope].length);
      expect(it, scope).toEqual([...it].sort((a, b) => a - b));
      expect(it.map((i) => TOUR_STEPS[i].key), scope).toEqual(
        TOUR_STEPS.filter((s) => TOUR_SCOPES[scope].includes(s.key)).map((s) => s.key),
      );
      union.push(...it);
    }
    expect(union.sort((a, b) => a - b)).toEqual(TOUR_STEPS.map((_, i) => i));
    expect(tourItinerary(everything)).toEqual(TOUR_STEPS.map((_, i) => i));
  });

  it("drops a topic's steps when the viewer lacks its right, leaving other topics whole", () => {
    const noOrgSettings = (c: string) => c !== "org_settings";
    expect(tourItinerary(noOrgSettings, "publish").map((i) => TOUR_STEPS[i].key)).not.toContain(
      "stepPublish",
    );
    expect(tourItinerary(noOrgSettings, "relationships")).toHaveLength(
      TOUR_SCOPES.relationships.length,
    );
  });
});

describe("Deep Dives completion marks", () => {
  beforeEach(() => localStorage.clear());

  it("remembers completed topics once each, per viewer", () => {
    expect(completedTopics()).toEqual([]);
    markTopicCompleted("query");
    markTopicCompleted("query");
    markTopicCompleted("govern");
    expect(completedTopics()).toEqual(["query", "govern"]);
  });

  it("ignores stored ids that are not topics", () => {
    localStorage.setItem(TOUR_TOPICS_DONE_KEY, JSON.stringify(["govern", "bogus"]));
    expect(completedTopics()).toEqual(["govern"]);
  });

  it("shows no marks, and does not throw, when the browser refuses storage", () => {
    const get = vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new DOMException("denied", "SecurityError");
    });
    const set = vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new DOMException("denied", "SecurityError");
    });
    expect(completedTopics()).toEqual([]);
    expect(() => markTopicCompleted("govern")).not.toThrow();
    get.mockRestore();
    set.mockRestore();
  });
});
