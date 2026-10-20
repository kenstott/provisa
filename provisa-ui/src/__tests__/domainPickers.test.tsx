// Copyright (c) 2026 Kenneth Stott
// Canary: 0266b8e4-e6a0-404f-b400-3998c8946d34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Every picker that chooses the domain something is put into leaves out meta and ops, the domains
// Provisa generates for itself (lib/internalDomains), whatever list its page hands it: the model
// form, both tabs of the New view form (the Metrics tab's own picker; the SQL tab saves through
// the SQL editor's view modal), the data product form and the command form.

import { useState } from "react";
import { describe, it, expect, vi } from "vitest";
import { render, screen, within } from "../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { Domain } from "../types/admin";

vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useTables: () => TABLES,
  useMetrics: () => METRICS,
  useRegisterEntity: () => ({ registerEntity: vi.fn() }),
  useRegisterFact: () => ({ registerFact: vi.fn() }),
}));
// One value per hook across renders: a fresh array each render reruns the forms' effects.
const TABLES = { tables: [], loading: false, refetch: vi.fn() };
const METRICS = { metrics: [], loading: false, error: undefined, refetch: vi.fn() };

import { ModelingForm } from "../pages/tables/ModelingForm";
import { ViewDefinitionForm } from "../pages/tables/ViewDefinitionForm";
import { ViewModal } from "../pages/sql/ViewModal";
import { DataProductFormCard } from "../pages/data-products/DataProductFormCard";
import { EMPTY_FORM as EMPTY_PRODUCT, type DataProductForm } from "../pages/data-products/types";
import { CommandFormFields } from "../pages/commands/CommandFormFields";
import { EMPTY_FORM as EMPTY_COMMAND, type FormState } from "../pages/commands/types";

const IDS = ["sales", "meta", "ops", "finance"];
const DOMAINS: Domain[] = IDS.map(
  (id) => ({ id, description: "", isSystem: id === "meta" || id === "ops" }) as unknown as Domain,
);

/** The options a Mantine Select offers once opened. */
async function offered(testId: string): Promise<string[]> {
  const combobox = screen.getByTestId(testId);
  await userEvent.click(combobox);
  const listbox = document.getElementById(combobox.getAttribute("aria-controls") ?? "");
  if (!listbox) throw new Error(`${testId} opened no list`);
  return within(listbox)
    .queryAllByRole("option", { hidden: true })
    .map((o) => o.textContent ?? "");
}

function expectAssignable(options: string[]) {
  expect(options.some((o) => o.startsWith("sales"))).toBe(true);
  expect(options.some((o) => o.startsWith("finance"))).toBe(true);
  expect(options.filter((o) => /^(meta|ops)\b/.test(o))).toEqual([]);
}

describe("the pickers of the domain something is put into", () => {
  it("the New model form", async () => {
    render(<ModelingForm domains={DOMAINS} onSuccess={vi.fn()} onCancel={vi.fn()} />);
    expectAssignable(await offered("modeling-domain"));
  });

  it("the New view form's Metrics tab", async () => {
    render(
      <ViewDefinitionForm
        editing={null}
        tables={[]}
        relationships={[]}
        domainHints={IDS}
        registerTable={vi.fn()}
        updateTable={vi.fn()}
        onSuccess={vi.fn()}
        onCancel={vi.fn()}
      />,
    );
    await userEvent.click(screen.getByRole("radio", { name: "Metrics" }));
    expectAssignable(await offered("view-definition-domain"));
  });

  it("the New view form's SQL tab, saved through the SQL editor's view modal", async () => {
    const noop = () => undefined;
    render(
      <ViewModal
        viewModal
        setViewModal={noop}
        viewMsg=""
        canCreateView
        handleSaveView={noop}
        viewSaving={false}
        viewId=""
        setViewId={noop}
        viewDomainId=""
        setViewDomainId={noop}
        viewHasParams={false}
        viewDescription=""
        setViewDescription={noop}
        viewRegion={null}
        setViewRegion={noop}
        regions={[]}
        viewSqlNormalized="SELECT 1"
        viewSqlExtensions={[]}
        domainMap={Object.fromEntries(DOMAINS.map((d) => [d.id, d]))}
        metricViewInfo={null}
        saveAsMetricView={false}
        setSaveAsMetricView={noop}
        savedViewId={null}
        setSavedViewId={noop}
        setViewColumns={noop}
        onNavigateToViews={noop}
        onCloseConfirmation={noop}
      />,
    );
    expectAssignable(await offered("view-domain-select"));
  });

  it("the data product form", async () => {
    function Harness() {
      const [form, setForm] = useState<DataProductForm>(EMPTY_PRODUCT);
      return (
        <DataProductFormCard
          editingId={null}
          form={form}
          setForm={setForm}
          domainOptions={IDS}
          roleOptions={["admin"]}
          tables={[]}
          selectedTableIds={[]}
          setSelectedTableIds={vi.fn()}
          functions={[]}
          selectedFunctionNames={[]}
          setSelectedFunctionNames={vi.fn()}
          saving={false}
          msg=""
          onSave={vi.fn()}
          onCancel={vi.fn()}
        />
      );
    }
    render(<Harness />);
    expectAssignable(await offered("data-product-domain-input"));
  });

  it("the command form", async () => {
    function Harness() {
      const [form, setForm] = useState<FormState>(EMPTY_COMMAND);
      return (
        <CommandFormFields
          form={form}
          setForm={setForm}
          sources={[]}
          tables={[]}
          domainHints={IDS}
          availableFunctions={[]}
          loadingFunctions={false}
          dataProducts={[]}
          roleOptions={[]}
        />
      );
    }
    render(<Harness />);
    expectAssignable(await offered("command-domain-select"));
  });
});
