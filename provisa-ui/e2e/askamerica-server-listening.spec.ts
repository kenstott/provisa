// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What a host owes an AskAmerica source: saved through the UI, its bundled server is started and
// listens. That is the launcher, the bundle's own Python and the JVM it starts all running on the
// host. How long the server then takes to answer its first catalog query is the server's own
// business and is the full case's to judge (source-to-query-special-cases.spec.ts).

import { test, expect } from "./coverage";
import {
  ASKAMERICA_SERVING_BUDGET_MS,
  openSourcesForm,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";

const LISTENING = "is listening and answering its first catalog query";

test.describe("govdata: a saved source's server listens", () => {
  const apiKey = process.env.FREE_ASKAMERICA_KEY ?? "";

  test("add the source; its server is listening within a minute", async ({ page }) => {
    test.skip(!apiKey, "FREE_ASKAMERICA_KEY not set: AskAmerica is a live API with no mock.");
    test.setTimeout(ASKAMERICA_SERVING_BUDGET_MS + 180000);
    const sourceId = `e2e_govdata_${Date.now()}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("govdata");
    await page.getByTestId("govdata-subject-WEATHER").check();
    await page.getByTestId("govdata-api-key-input").fill(apiKey);
    await submitSourceAndExpectListed(page, sourceId);

    // The listing answers once the server serves, and STARTING until then: "still starting up"
    // while its port is closed, LISTENING once it is open. Any other error is the server's failure.
    await expect
      .poll(
        async () => {
          const res = await page.request.post("/admin/graphql", {
            data: {
              query:
                "query($sourceId: String!, $schema: String!) { availableTables(sourceId: $sourceId, schemaName: $schema) { name } }",
              variables: { sourceId, schema: "weather" },
            },
          });
          expect(res.ok(), await res.text()).toBeTruthy();
          const body = await res.json();
          if (!body.errors?.length) return "serving";
          const message = body.errors.map((e: { message: string }) => e.message).join("; ");
          expect(message, message).toContain("STARTING:");
          return message.includes(LISTENING) ? "listening" : "starting";
        },
        { timeout: ASKAMERICA_SERVING_BUDGET_MS, intervals: [5000] },
      )
      .toMatch(/^(listening|serving)$/);
  });
});
