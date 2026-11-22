// Copyright (c) 2026 Kenneth Stott
// Canary: 2c9f7a41-d8b3-4e65-91a0-f63e5b2d8c17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: Context Health against the real backend -- the steward summary and every card read
// from the server; a draft made here is counted among the drafts in review, and the count links
// to the Ontology page. The term is made under a name unique to the attempt and deleted after.

import { test, expect } from "./coverage";

test("Context Health shows the steward summary and a card per signal, read from the server", async ({
  page,
  request,
}, testInfo) => {
  const before = await request.get("/admin/glossary/terms?include_deprecated=false");
  if (!before.ok()) throw new Error(`listing terms: ${before.status()} ${await before.text()}`);
  const terms = (await before.json()) as { name: string; status: string; domains: string[] }[];
  const drafts = terms.filter((t) => t.status === "draft").length;
  // A term needs at least one domain; the draft sits in the seeded "species" term's domains.
  const species = terms.find((t) => t.name === "species");
  if (!species) throw new Error('no glossary term "species"');
  const tag = `ch${Date.now().toString(36)}${testInfo.retry}`;
  const made = await request.post("/admin/glossary/terms", {
    data: { name: `${tag} draft`, definition: "Waiting for a steward.", domains: species.domains },
  });
  if (!made.ok()) throw new Error(`creating a draft: ${made.status()} ${await made.text()}`);
  const id = ((await made.json()) as { id: number }).id;
  try {
    await page.goto("/");
    await page.getByTestId("nav-group-context").click();
    await page
      .locator(".subnav")
      .getByRole("link", { name: "Context Health", exact: true })
      .click();
    await expect(page).toHaveURL(/\/context\/health$/);

    await expect(page.getByTestId("glossary-stewardship")).toBeVisible({ timeout: 60_000 });
    for (const card of ["terms", "drift", "tags-lost", "classification", "documents", "sync"]) {
      await expect(page.getByTestId(`health-${card}`)).toBeVisible();
      await expect(page.getByTestId(`health-${card}-error`)).toHaveCount(0);
    }
    const draftCount = page.getByTestId("health-terms-drafts");
    await expect(draftCount).toHaveText(String(drafts + 1));
    await draftCount.click();
    await expect(page).toHaveURL(/\/context\/ontology$/);
  } finally {
    // A delete retires the term (REQ-2248); the page leaves for the Glossary, which reads its
    // entries once and asks nothing per term, before the term goes.
    await page.goto("/context/glossary");
    await expect(page.getByTestId("glossary-page")).toBeVisible({ timeout: 60_000 });
    expect((await request.delete(`/admin/glossary/terms/${id}`)).ok()).toBe(true);
  }
});
