// Copyright (c) 2026 Kenneth Stott
// Canary: 7b3e9d15-2c6a-4f81-a0d4-58e1c9b2f6a7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: the read-only Glossary page against the real backend. An approved term is shown with
// its definition and its broader term as a link within the page; a draft is not shown. The terms
// are made here, under names unique to the attempt, and deleted after.

import type { APIRequestContext } from "@playwright/test";
import { test, expect } from "./coverage";

async function created(request: APIRequestContext, body: object): Promise<number> {
  const res = await request.post("/admin/glossary/terms", { data: body });
  if (!res.ok())
    throw new Error(`creating ${JSON.stringify(body)}: ${res.status()} ${await res.text()}`);
  return ((await res.json()) as { id: number }).id;
}

async function ok(what: string, res: Awaited<ReturnType<APIRequestContext["post"]>>) {
  if (!res.ok()) throw new Error(`${what}: ${res.status()} ${await res.text()}`);
}

/** The declared domains of a seeded term, so the made terms sit where the config's terms sit. */
async function domainsOf(request: APIRequestContext, name: string): Promise<string[]> {
  const res = await request.get(`/admin/glossary/terms?q=${encodeURIComponent(name)}`);
  await ok(`listing "${name}"`, res);
  const term = ((await res.json()) as { name: string; domains: string[] }[]).find(
    (t) => t.name === name,
  );
  if (!term) throw new Error(`no glossary term "${name}"`);
  return term.domains;
}

test("the Glossary shows approved terms with their definitions and related terms, not drafts", async ({
  page,
  request,
}, testInfo) => {
  const tag = `gl${Date.now().toString(36)}${testInfo.retry}`;
  const domains = await domainsOf(request, "species");
  const animal = await created(request, {
    name: `${tag} animal`,
    definition: "A living creature.",
    domains,
  });
  const pet = await created(request, {
    name: `${tag} pet`,
    definition: "An animal kept at home.",
    domains,
  });
  const draft = await created(request, {
    name: `${tag} stray`,
    definition: "Not yet approved.",
    domains,
  });
  try {
    await ok("approving animal", await request.post(`/admin/glossary/terms/${animal}/approve`));
    await ok("approving pet", await request.post(`/admin/glossary/terms/${pet}/approve`));
    await ok(
      "pet is a kind of animal",
      await request.post(`/admin/glossary/terms/${pet}/edges`, {
        data: { to_term_id: animal, rel_type: "KIND_OF" },
      }),
    );

    await page.goto("/");
    await page.getByTestId("nav-group-context").click();
    await page.locator(".subnav").getByRole("link", { name: "Glossary", exact: true }).click();
    await expect(page).toHaveURL(/\/context\/glossary$/);
    await expect(page.getByTestId("glossary-page")).toBeVisible({ timeout: 60_000 });

    await page.getByTestId("glossary-search").fill(tag);
    await expect(page.getByTestId(`glossary-definition-${pet}`)).toHaveText(
      "An animal kept at home.",
    );
    await expect(page.getByTestId(`glossary-kindof-${pet}`)).toContainText(
      `A kind of: ${tag} animal`,
    );
    await expect(page.getByTestId(`glossary-kinds-${animal}`)).toContainText(`Kinds: ${tag} pet`);
    await expect(page.getByTestId(`glossary-sense-${draft}`)).toHaveCount(0);
    await expect(page.getByTestId("glossary-count")).toHaveText("2 terms");

    // The name opens the term in the Ontology editor.
    await page.getByTestId(`glossary-name-${pet}`).click();
    await expect(page).toHaveURL(new RegExp(`/context/ontology\\?term=${pet}$`));
  } finally {
    for (const id of [pet, animal, draft]) await request.delete(`/admin/glossary/terms/${id}`);
  }
});
