// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e7d21-3b5a-4f8e-a1d6-7e2b0c9f4a83
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import type { Page } from "@playwright/test";

/**
 * REQ-1387: admin business-glossary curation, end to end against the real backend.
 *
 * Nothing is stubbed. The e2e config registers the PetStore/Shelter tables, and
 * table registration derives glossary terms from column names via normalize_term
 * (provisa/core/glossary.py): employee_id → "employee", species → "species",
 * breed_name → "breed name". Those rooted terms are guaranteed present before any
 * test runs, because global-setup waits for the schema rebuild.
 *
 * Retry safety (retries: 1 re-runs a failed test against the already-mutated
 * backend): the rename test locates the term under either its original or its
 * renamed name; created abstract terms use a per-attempt unique name; expert and
 * edge additions are server-side upserts.
 */

// Rooted terms derived from the seeded model's columns.
const DERIVED_EMPLOYEE = "employee"; // employee_id
const DERIVED_SPECIES = "species"; // species
const DERIVED_BREED_NAME = "breed name"; // breed_name

const RENAMED_EMPLOYEE = "workforce member";
const EMPLOYEE_DEFINITION = "A person employed by the shelter to care for animals.";

/**
 * Open /admin/ontology and wait for the term list to be populated. The admin page
 * is a lazily-compiled Vite chunk, so the first navigation of the run can exceed
 * the 5 s default expect timeout — the generous wait is on first paint only.
 */
async function openGlossary(page: Page) {
  await page.goto("/admin/ontology");
  await expect(page.getByTestId("glossary-search")).toBeVisible({ timeout: 60000 });
  // The list has loaded once at least one derived term row is rendered.
  await expect(
    page.getByTestId("glossary-list").getByText(DERIVED_SPECIES, { exact: true }),
  ).toBeVisible({ timeout: 30000 });
}

/** Click a term in the left-hand list and wait for its detail panel, read. */
async function selectTerm(page: Page, name: string) {
  await page.getByTestId("glossary-list").getByText(name, { exact: true }).click();
  await expect(page.getByTestId("glossary-read-fields")).toContainText(name);
}

/** Open the term shown as one form. */
async function editTerm(page: Page) {
  await page.getByTestId("glossary-edit-btn").click();
  await expect(page.getByTestId("glossary-edit-form")).toBeVisible();
}

/** Save the form, and wait for the term to be read again. */
async function saveTerm(page: Page) {
  await page.getByTestId("glossary-edit-save").click();
  await expect(page.getByTestId("glossary-edit-form")).toHaveCount(0);
}

// Every test here starts with openGlossary, whose two waits alone budget 90 s for the first paint
// of the lazily-compiled admin chunk plus the term list. Under the 30 s default per-test timeout
// those budgets could never be spent: the expert test reached its post-reload wait — itself written
// for 60 s — with the test clock already exhausted and died inside page.reload().
test.describe.configure({ timeout: 180_000 });

test.describe("REQ-1387 glossary curation", () => {
  test("lists terms derived from the registered model's columns", async ({ page }) => {
    await openGlossary(page);

    const list = page.getByTestId("glossary-list");
    // species (Breeds.species) and breed name (EmployeeAssignments.breed_name) are
    // stable across this spec; employee may carry the renamed name on a retry.
    await expect(list.getByText(DERIVED_SPECIES, { exact: true })).toBeVisible();
    await expect(list.getByText(DERIVED_BREED_NAME, { exact: true })).toBeVisible();

    // Derived terms are rooted: selecting one shows its physical refs.
    await selectTerm(page, DERIVED_SPECIES);
    await expect(page.locator('[data-testid^="glossary-ref-"]').first()).toBeVisible();
  });

  test("search filters the term list server-side", async ({ page }) => {
    await openGlossary(page);

    const list = page.getByTestId("glossary-list");
    await expect(list.getByText(DERIVED_BREED_NAME, { exact: true })).toBeVisible();

    await page.getByTestId("glossary-search").fill(DERIVED_SPECIES);
    await expect(list.getByText(DERIVED_SPECIES, { exact: true })).toBeVisible();
    await expect(list.getByText(DERIVED_BREED_NAME, { exact: true })).toHaveCount(0);

    await page.getByTestId("glossary-search").fill("");
    await expect(list.getByText(DERIVED_BREED_NAME, { exact: true })).toBeVisible();
  });

  test("rename and definition persist across a reload", async ({ page }) => {
    await openGlossary(page);

    // On a retry the rename has already happened; select whichever name is live.
    const list = page.getByTestId("glossary-list");
    const item = list
      .getByText(RENAMED_EMPLOYEE, { exact: true })
      .or(list.getByText(DERIVED_EMPLOYEE, { exact: true }))
      .first();
    await item.click();
    await expect(page.getByTestId("glossary-read-fields")).toBeVisible();
    await editTerm(page);
    const nameInput = page.getByTestId("glossary-name-input");
    await expect(nameInput).not.toHaveValue("");

    // The deployment keeps what an earlier run wrote, so a retry may have nothing to change: Save
    // with nothing changed reads the term again all the same, and the reload below is what proves
    // persistence either way.
    await nameInput.fill(RENAMED_EMPLOYEE);
    await page
      .getByTestId("glossary-definition-input")
      .getByRole("textbox")
      .fill(EMPLOYEE_DEFINITION);
    await saveTerm(page);
    await expect(list.getByText(RENAMED_EMPLOYEE, { exact: true })).toBeVisible();

    await page.reload();
    await expect(page.getByTestId("glossary-search")).toBeVisible({ timeout: 60000 });
    await selectTerm(page, RENAMED_EMPLOYEE);
    await expect(page.getByTestId("glossary-read-fields")).toContainText(EMPLOYEE_DEFINITION);
  });

  test("an abstract term takes a KIND_OF edge to a rooted term", async ({ page }) => {
    await openGlossary(page);

    // Unique per attempt: create_abstract_term refuses duplicate names, and a
    // retry re-runs against the backend that already holds the first attempt's term.
    const abstractName = `animal concept ${Date.now()}`;

    await page.getByTestId("glossary-new-btn").click();
    await page.getByTestId("glossary-add-name-input").fill(abstractName);
    await page
      .getByTestId("glossary-add-definition-input")
      .fill("Umbrella concept for animal classification terms.");
    // REQ-1591: an abstract term has no refs to derive its domains from, so it declares them and
    // the modal will not save without one. This deployment is multi-domain, so the field is there.
    // The click lands on the MultiSelect's box, not on the element carrying the test id: this
    // picker is not `searchable`, so Mantine renders its field as a zero-width hidden input and
    // the box painted over it takes every pointer event aimed at the field's centre. The box is
    // also what a curator clicks.
    await page
      .locator(".mantine-MultiSelect-input")
      .filter({ has: page.getByTestId("glossary-add-domains-input") })
      .click();
    await page.getByRole("option", { name: "shelter", exact: true }).click();
    // The MultiSelect keeps its dropdown open after a pick, and it overlays the save button.
    await page.keyboard.press("Escape");
    await page.getByTestId("glossary-add-save-btn").click();

    // Creation selects the new term; it is a concept, so it has no refs.
    await expect(page.getByTestId("glossary-read-fields")).toContainText(abstractName);
    await expect(page.getByTestId("glossary-detail").getByText("No physical refs.")).toBeVisible();

    // KIND_OF edge to the rooted "species" term, in the term's Edit form.
    await editTerm(page);
    await page.getByTestId("glossary-edge-rel-select").click();
    await page.getByRole("option", { name: "Kind of" }).click();
    await page.getByTestId("glossary-edge-term-select").click();
    await page.getByTestId("glossary-edge-term-select").fill(DERIVED_SPECIES);
    await page.getByRole("option", { name: DERIVED_SPECIES, exact: true }).click();
    await page.getByTestId("glossary-edge-add-btn").click();
    await saveTerm(page);

    // Read again: the edge's type is said as text.
    const edge = page.locator('[data-testid^="glossary-edge-out-"][data-testid$="-KIND_OF"]');
    await expect(edge).toBeVisible();
    await expect(edge).toContainText("Kind of");
    await expect(edge).toContainText(DERIVED_SPECIES);

    // REQ-1643: a term with a relationship cannot be deleted -- the button is disabled and the
    // hint says why. (The abstract term has no refs; the edge alone is enough to block it.)
    await expect(page.getByTestId("glossary-delete-btn")).toBeDisabled();
    await page.getByTestId("glossary-delete-btn").hover({ force: true });
    await expect(
      page.getByText("This term has refs or relationships", { exact: false }),
    ).toBeVisible();

    // The inverse direction is visible from the rooted term.
    await selectTerm(page, DERIVED_SPECIES);
    // REQ-1643: the rooted term carries physical refs (and now an incoming edge): also undeletable.
    await expect(page.getByTestId("glossary-delete-btn")).toBeDisabled();
    // One incoming edge per run of this test survives on a deployment this suite does not own, so
    // the assertion is that THIS run's abstract term is among them, not that it is the only one.
    const incoming = page
      .locator('[data-testid^="glossary-edge-in-"][data-testid$="-KIND_OF"]')
      .filter({ hasText: abstractName });
    await expect(incoming).toHaveCount(1);
  });

  test("an expert attaches to a term and shows in its detail", async ({ page }) => {
    await openGlossary(page);
    await selectTerm(page, DERIVED_SPECIES);

    // add_expert is a server-side upsert, so a retry re-adding the same user passes.
    // REQ-1592: the expert is PICKED from the org roster, not typed — typing into the Select's
    // search box leaves the chosen user unset and the Add button disabled. global-setup seeds this
    // person into every backend's org so there is someone to pick.
    // The picker is in the term's Edit form.
    await editTerm(page);
    await page.getByTestId("glossary-expert-user-input").click();
    await page.getByRole("option", { name: "jane.doe", exact: true }).click();
    await page.getByTestId("glossary-expert-kind-select").click();
    await page.getByRole("option", { name: "Expert" }).click();
    await page.getByTestId("glossary-expert-add-btn").click();
    await saveTerm(page);

    const expert = page.getByTestId("glossary-expert-jane.doe");
    await expect(expert).toBeVisible();
    await expect(expert).toContainText("jane.doe");
    await expect(expert).toContainText("Expert");

    // Persistence, not just optimistic state: the expert survives a reload.
    await page.reload();
    await expect(page.getByTestId("glossary-search")).toBeVisible({ timeout: 60000 });
    await selectTerm(page, DERIVED_SPECIES);
    await expect(page.getByTestId("glossary-expert-jane.doe")).toBeVisible();
  });

  test("a rooted term with refs cannot be deleted", async ({ page }) => {
    await openGlossary(page);
    await selectTerm(page, DERIVED_BREED_NAME);

    // Rooted terms are lifecycle-managed by the semantic layer; the UI blocks the
    // delete outright rather than round-tripping to the server's 400.
    await expect(page.locator('[data-testid^="glossary-ref-"]').first()).toBeVisible();
    await expect(page.getByTestId("glossary-delete-btn")).toBeDisabled();
  });
});
