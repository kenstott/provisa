// Copyright (c) 2026 Kenneth Stott
// Canary: c4d1e870-2a6b-49f3-8d15-7b3f90ea26d4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Polly page parity: the ontology page's main panel, its term and its graph's controls can be set
// by payload or URL, whether the page was just opened or already open, and publish to read_screen.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, useNavigate } from "react-router-dom";
import { useGlossaryPanel } from "../components/admin/useGlossaryPanel";
import { readScreen } from "../mcp/screenState";
import { ONTOLOGY_SETTINGS, savedGraphSettings } from "../pages/tables/profile3d/graphSettings";

function Page() {
  const { ontology, selectedId } = useGlossaryPanel(false);
  return <div data-testid="page">{`${ontology ? "graph" : "terms"}:${selectedId}`}</div>;
}

function Go({ label, to, state }: { label: string; to: string; state?: object }) {
  const navigate = useNavigate();
  return <button onClick={() => navigate(to, { state })}>{label}</button>;
}

async function inputs() {
  const read = await readScreen(500);
  if (typeof read === "string") throw new Error(read);
  return read.inputs;
}

beforeEach(() => {
  HTMLElement.prototype.checkVisibility = () => true;
  localStorage.clear();
  window.history.pushState({}, "", "/admin/ontology");
});
afterEach(() => cleanup());

describe("the ontology page", () => {
  it("opens on the panel and term a link names, and again when the link changes", async () => {
    render(
      <MemoryRouter initialEntries={["/admin/ontology?panel=graph"]}>
        <Go label="term" to="/admin/ontology?term=7" />
        <Go label="graph" to="/admin/ontology?panel=graph" />
        <Page />
      </MemoryRouter>,
    );
    expect(screen.getByTestId("page")).toHaveTextContent("graph:null");
    expect(await inputs()).toMatchObject({ panel: "graph", term: null });

    act(() => screen.getByText("term").click());
    await waitFor(() => expect(screen.getByTestId("page")).toHaveTextContent("terms:7"));
    expect(await inputs()).toMatchObject({ panel: "terms", term: 7 });

    act(() => screen.getByText("graph").click());
    await waitFor(() => expect(screen.getByTestId("page")).toHaveTextContent("graph:7"));
  });

  it("takes a payload: the panel, a term, and the graph's controls", async () => {
    const graph = { flat: true, layout: "layered", layers: ["ontology", "data"] };
    render(
      <MemoryRouter initialEntries={["/admin/ontology"]}>
        <Go label="graph" to="/admin/ontology" state={{ panel: "graph", graph }} />
        <Go label="term" to="/admin/ontology" state={{ term: 3 }} />
        <Page />
      </MemoryRouter>,
    );
    act(() => screen.getByText("graph").click());
    await waitFor(() => expect(screen.getByTestId("page")).toHaveTextContent("graph:null"));
    expect(savedGraphSettings(ONTOLOGY_SETTINGS)).toMatchObject(graph);
    expect(await inputs()).toMatchObject({ panel: "graph", graph });

    act(() => screen.getByText("term").click());
    await waitFor(() => expect(screen.getByTestId("page")).toHaveTextContent("terms:3"));
  });
});
