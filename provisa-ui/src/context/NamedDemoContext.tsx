// Copyright (c) 2026 Kenneth Stott
// Canary: c8df6bc9-ca2c-4053-a1bc-089755d52a31
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { createContext, useContext } from "react";
import type { ExampleQuery, SetupStatus } from "../api/setup";
import type { DemoLanding, DemoTourStepDef } from "../tour/demoTour";

/** The named demo this deployment was started as, from /setup/status (App.tsx provides it): its
 * name, its example GraphQL queries (REQ-2009) and its own tour (REQ-2010). */
export interface NamedDemo {
  name: string | null;
  exampleQueries: ExampleQuery[];
  tour: DemoTourStepDef[];
  /** Where the tour's last "Look around on my own" leaves the visitor; null with no tour. */
  landing: DemoLanding | null;
  /** REQ-2084: the code cells of the notebook the demo ships; none when it ships none. */
  notebook: string[];
}

/** A page rendered outside the app (a component test) has read no status: no named demo. */
export const NO_NAMED_DEMO: NamedDemo = {
  name: null,
  exampleQueries: [],
  tour: [],
  landing: null,
  notebook: [],
};

/** The named demo a status reports. A deployment started as no named demo (a fresh org's) has
 * none: no demo track and no examples, whatever else the status carries. */
export function namedDemoOf(
  status: Pick<
    SetupStatus,
    "named_demo" | "example_queries" | "demo_tour" | "demo_landing" | "demo_notebook"
  >,
): NamedDemo {
  if (status.named_demo === null) return NO_NAMED_DEMO;
  return {
    name: status.named_demo,
    exampleQueries: status.example_queries,
    tour: status.demo_tour,
    landing: status.demo_landing,
    notebook: status.demo_notebook,
  };
}

export const NamedDemoContext = createContext<NamedDemo>(NO_NAMED_DEMO);

export function useNamedDemo(): NamedDemo {
  return useContext(NamedDemoContext);
}
