// Copyright (c) 2026 Kenneth Stott
// Canary: e0ffcde8-86b5-4293-a926-09d6a2a0e3d8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2241: a detail panel is in the address. Opening a row writes its object's parameter and
// choosing a tab writes the tab, each as a history entry; closing removes them; loading the URL,
// Back and Forward restore the same panel on the same tab.

import { describe, it, expect } from "vitest";
import { Table, Tabs } from "@mantine/core";
import userEvent from "@testing-library/user-event";
import { MantineProvider } from "@mantine/core";
import { I18nextProvider } from "react-i18next";
import { MemoryRouter, useLocation, useNavigate } from "react-router-dom";
import { render, screen, within } from "@testing-library/react";
import i18n from "../../../i18n";
import { theme } from "../../../theme/theme";
import { ListRow } from "../ListTable";
import { SplitPane } from "../SplitPane";
import { useDetailParam } from "../useDetailParam";

const LABELS = {
  panel: "Details",
  resize: "Resize",
  expand: "Expand",
  restore: "Restore",
  close: "Close",
  backToList: "Back",
};
const ROWS = ["vets", "owners", "pets"];

function Where() {
  const location = useLocation();
  const navigate = useNavigate();
  return (
    <>
      <output data-testid="where">{location.pathname + location.search}</output>
      <button onClick={() => navigate(-1)}>history-back</button>
      <button onClick={() => navigate(1)}>history-forward</button>
    </>
  );
}

function Page() {
  const detail = useDetailParam("tables");
  const section = detail.tab ?? "identity";
  return (
    <SplitPane
      storageKey="provisa.test.detailParam"
      labels={LABELS}
      testId="dl"
      title={detail.id}
      onClose={() => detail.close()}
      list={
        <Table>
          <Table.Tbody>
            {ROWS.map((name) => (
              <ListRow
                key={name}
                itemKey={name}
                selected={detail.id === name}
                onClick={() => detail.open(name)}
              >
                <Table.Td>{name}</Table.Td>
              </ListRow>
            ))}
          </Table.Tbody>
        </Table>
      }
    >
      {detail.id === null ? null : (
        <Tabs value={section} onChange={(v) => v && detail.setTab(v)}>
          <Tabs.List>
            <Tabs.Tab value="identity">Identity</Tabs.Tab>
            <Tabs.Tab value="columns">Columns</Tabs.Tab>
            <Tabs.Tab value="quality">Quality</Tabs.Tab>
          </Tabs.List>
        </Tabs>
      )}
    </SplitPane>
  );
}

function show(entry = "/tables") {
  return render(
    <MantineProvider theme={theme}>
      <I18nextProvider i18n={i18n}>
        <MemoryRouter initialEntries={[entry]}>
          <Where />
          <Page />
        </MemoryRouter>
      </I18nextProvider>
    </MantineProvider>,
  );
}

const where = () => screen.getByTestId("where").textContent;
const title = () => screen.getByTestId("dl-title").textContent;
const activeTab = () =>
  within(screen.getByTestId("dl-panel")).getByRole("tab", { selected: true }).textContent;

describe("useDetailParam (REQ-2241)", () => {
  it("opening a row writes the object's parameter", async () => {
    show("/tables?source=pets");
    await userEvent.click(screen.getByText("vets"));
    expect(where()).toBe("/tables?source=pets&table=vets");
    expect(title()).toBe("vets");
  });

  it("choosing a tab writes it", async () => {
    show();
    await userEvent.click(screen.getByText("vets"));
    await userEvent.click(screen.getByRole("tab", { name: "Columns" }));
    expect(where()).toBe("/tables?table=vets&section=columns");
    expect(activeTab()).toBe("Columns");
  });

  it("Back restores the previous panel and tab, and Forward the next", async () => {
    show();
    await userEvent.click(screen.getByText("vets"));
    await userEvent.click(screen.getByRole("tab", { name: "Quality" }));
    await userEvent.click(screen.getByText("owners"));
    expect(where()).toBe("/tables?table=owners&section=quality");
    expect(activeTab()).toBe("Quality");

    await userEvent.click(screen.getByText("history-back"));
    expect(where()).toBe("/tables?table=vets&section=quality");
    expect(title()).toBe("vets");
    expect(activeTab()).toBe("Quality");

    await userEvent.click(screen.getByText("history-back"));
    expect(where()).toBe("/tables?table=vets");
    expect(activeTab()).toBe("Identity");

    await userEvent.click(screen.getByText("history-back"));
    expect(where()).toBe("/tables");
    expect(screen.queryByTestId("dl-panel")).toBeNull();

    await userEvent.click(screen.getByText("history-forward"));
    expect(title()).toBe("vets");
  });

  it("loading a URL opens the panel on that tab", () => {
    show("/tables?table=pets&section=quality");
    expect(title()).toBe("pets");
    expect(activeTab()).toBe("Quality");
  });

  it("closing removes the parameters, and Back reopens the panel", async () => {
    show("/tables?source=x&table=pets&section=columns&column=name");
    await userEvent.click(screen.getByTestId("dl-close"));
    expect(where()).toBe("/tables?source=x");
    expect(screen.queryByTestId("dl-panel")).toBeNull();

    await userEvent.click(screen.getByText("history-back"));
    expect(title()).toBe("pets");
    expect(activeTab()).toBe("Columns");
  });

  it("opening another object keeps the tab and drops what belonged to the one before", async () => {
    show("/tables?table=pets&section=columns&column=name");
    await userEvent.click(screen.getByText("vets"));
    expect(where()).toBe("/tables?table=vets&section=columns");
    expect(activeTab()).toBe("Columns");
  });

  it("choosing the tab already shown, or the row already open, writes nothing new", async () => {
    show("/tables?table=pets");
    await userEvent.click(within(screen.getByRole("table")).getByText("pets"));
    await userEvent.click(screen.getByRole("tab", { name: "Identity" }));
    expect(where()).toBe("/tables?table=pets");
    await userEvent.click(screen.getByText("history-back"));
    // Nothing was pushed: Back has nowhere to go and the panel stays.
    expect(title()).toBe("pets");
  });

  it("an open and a tab chosen in one handler land as one entry", async () => {
    function OneHandler() {
      const detail = useDetailParam("tables");
      return (
        <button
          onClick={() => {
            detail.open("vets");
            detail.setTab("quality");
          }}
        >
          both
        </button>
      );
    }
    render(
      <MemoryRouter initialEntries={["/tables"]}>
        <Where />
        <OneHandler />
      </MemoryRouter>,
    );
    await userEvent.click(screen.getByText("both"));
    expect(where()).toBe("/tables?table=vets&section=quality");
    await userEvent.click(screen.getByText("history-back"));
    expect(where()).toBe("/tables");
  });
});
