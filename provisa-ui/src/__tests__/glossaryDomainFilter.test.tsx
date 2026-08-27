// Copyright (c) 2026 Kenneth Stott
// Canary: 5d0c9a41-7e26-4f83-9b12-0a6f3e5c8d47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1591: the navbar domain selection is a VIEW preference over the glossary, not authority. It
// narrows what the list asks for; the server intersects it with the role's own domain access and
// can never be widened by it. Checking every box asks for no narrowing at all — that is a different
// request from naming every domain, and the distinction is what these cases pin. The other half of
// the requirement, an abstract term DECLARING its domains because it has no refs to derive from,
// is exercised through the create modal below.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import i18n from "../i18n";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermSummary } from "../api/glossary";

const t = i18n.getFixedT("en");

vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  createGlossaryTerm: vi.fn(),
}));

vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

// REQ-1592: mutated per case — the enterprise scope is offered to an `org_glossary_rw` holder
// alone, so the same mounted form must be checked from both sides of that right.
const auth = { capabilities: ["glossary_read", "glossary_rw"], activeOrgId: "acme" };
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

vi.mock("../api/admin", () => ({ fetchOrgMembers: vi.fn(async () => []) }));

// The navbar state under test. Mutating the object between cases is what lets "everything checked"
// and "one box unchecked" be two assertions against one mounted component rather than two suites.
const filter = {
  domains: ["sales", "hr"],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
  checkedDomains: new Set(["sales", "hr"]),
  toggleDomain: () => {},
  ensureDomainChecked: () => {},
  domainsEnabled: true,
};
vi.mock("../context/DomainFilterContext", () => ({ useDomainFilter: () => filter }));

import { createGlossaryTerm, listGlossaryTerms } from "../api/glossary";

const mockList = vi.mocked(listGlossaryTerms);
const mockCreate = vi.mocked(createGlossaryTerm);

const ORDER: GlossaryTermSummary = {
  id: 1,
  name: "Order",
  definition: "A customer's request to buy.",
  is_abstract: false,
  deprecated: false,
  ref_count: 2,
  export_excluded: false,
  retired: false,
  live: true,
  // REQ-1591: one term, two domains — the combined model. Its refs point at a sales table and a
  // pet-store table, and that is a single concept both domains reference, not two terms.
  domains: ["sales", "petstore"],
};

describe("glossary domain filter", () => {
  beforeEach(() => {
    mockList.mockReset();
    mockCreate.mockReset();
    mockList.mockResolvedValue([ORDER]);
    mockCreate.mockResolvedValue({ id: 9 });
    filter.domains = ["sales", "hr"];
    filter.checkedDomains = new Set(["sales", "hr"]);
    filter.domainsEnabled = true;
    auth.capabilities = ["glossary_read", "glossary_rw"];
  });

  it("asks for no narrowing when every domain is checked", async () => {
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    // Not ["sales","hr"]: naming every domain and asking for nothing look alike here but differ at
    // the server, where a role holding a domain the navbar does not list would lose it.
    expect(mockList).toHaveBeenCalledWith("", true, null);
  });

  it("narrows to the checked subset when a box is unchecked", async () => {
    filter.checkedDomains = new Set(["sales"]);
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    expect(mockList).toHaveBeenCalledWith("", true, ["sales"]);
  });

  it("asks for no narrowing at all in single-domain mode", async () => {
    filter.domainsEnabled = false;
    filter.checkedDomains = new Set(["sales"]);
    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    expect(mockList).toHaveBeenCalledWith("", true, null);
  });

  it("declares an abstract term's domains, because it has no refs to derive them from", async () => {
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-new-btn"));

    fireEvent.change(await screen.findByTestId("glossary-add-name-input"), {
      target: { value: "Customer" },
    });
    // The declaration is required in multi-domain mode: an unscoped term is reachable by every
    // glossary-right holder, so minting one with no domains would be a way around the gate.
    const save = screen.getByTestId("glossary-add-save-btn");
    expect(save).toBeDisabled();

    const input = screen.getByTestId("glossary-add-domains-input");
    fireEvent.click(input);
    const listbox = document.getElementById(
      input.getAttribute("aria-controls") as string,
    ) as HTMLElement;
    fireEvent.click(within_(listbox, "sales"));

    await waitFor(() => expect(save).not.toBeDisabled());
    fireEvent.click(save);
    await waitFor(() =>
      expect(mockCreate).toHaveBeenCalledWith({ name: "Customer", domains: ["sales"] }),
    );
  });

  // REQ-1592: "*" is the whole org, and it is the org's glossary owner who admits a term to it.
  it("offers the enterprise scope to the org's glossary owner alone", async () => {
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-new-btn"));
    const input = await screen.findByTestId("glossary-add-domains-input");
    fireEvent.click(input);
    const listbox = document.getElementById(
      input.getAttribute("aria-controls") as string,
    ) as HTMLElement;
    expect(listbox.textContent).not.toContain(t("glossaryTab.enterpriseDomainLabel"));
  });

  it("makes the enterprise scope exclusive of the named domains", async () => {
    auth.capabilities = ["glossary_read", "glossary_rw", "org_glossary_rw"];
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-new-btn"));
    fireEvent.change(await screen.findByTestId("glossary-add-name-input"), {
      target: { value: "Customer" },
    });

    const input = screen.getByTestId("glossary-add-domains-input");
    fireEvent.click(input);
    const listbox = document.getElementById(
      input.getAttribute("aria-controls") as string,
    ) as HTMLElement;
    fireEvent.click(within_(listbox, "sales"));
    fireEvent.click(within_(listbox, t("glossaryTab.enterpriseDomainLabel")));

    // The term is the whole org's or a named set of domains', never both: picking the enterprise
    // scope drops what was already chosen rather than adding to it.
    fireEvent.click(screen.getByTestId("glossary-add-save-btn"));
    await waitFor(() =>
      expect(mockCreate).toHaveBeenCalledWith({ name: "Customer", domains: ["*"] }),
    );
  });

  it("declares nothing in single-domain mode, where a domain gates nothing", async () => {
    filter.domainsEnabled = false;
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-new-btn"));

    fireEvent.change(await screen.findByTestId("glossary-add-name-input"), {
      target: { value: "Customer" },
    });
    expect(screen.queryByTestId("glossary-add-domains-input")).not.toBeInTheDocument();
    expect(screen.queryByText(t("glossaryTab.domainsLabel"))).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId("glossary-add-save-btn"));
    await waitFor(() => expect(mockCreate).toHaveBeenCalledWith({ name: "Customer" }));
  });
});

// Mantine's MultiSelect renders its dropdown detached, and floating-ui gives every option a zero
// rect in jsdom, so the visible-only role queries miss them. Scope to the listbox the input names
// through aria-controls and match on text.
function within_(listbox: HTMLElement, label: string): HTMLElement {
  const opt = [...listbox.querySelectorAll("*")].find((el) => el.textContent === label);
  if (!opt) throw new Error(`option ${label} not in dropdown`);
  return opt as HTMLElement;
}
