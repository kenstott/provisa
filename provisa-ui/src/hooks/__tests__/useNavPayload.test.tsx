// Copyright (c) 2026 Kenneth Stott
// Canary: 31709a15-dfae-46ca-b9d2-073e6f88c30c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A page is handed a value (a query to open, a method to select) as router state. The page may
// already be mounted when the hand-off arrives — Polly navigating to the page the user is on — so
// the payload is handled on every navigation that carries one, not once at mount. A handled payload
// is removed from the history entry, so a refresh does not hand it over again.

import { describe, it, expect, vi } from "vitest";
import { act, render, screen } from "@testing-library/react";
import { MemoryRouter, useLocation, useNavigate } from "react-router-dom";
import { useNavPayload } from "../useNavPayload";

let mounts = 0;

function Page({ onPayload }: { onPayload: (p: { sql: string }) => void }) {
  const [mountId] = [++mounts];
  void mountId;
  useNavPayload<{ sql: string }>(onPayload);
  const navigate = useNavigate();
  const location = useLocation();
  return (
    <div>
      <span data-testid="state">{JSON.stringify(location.state)}</span>
      <button onClick={() => navigate("/sql", { state: { sql: "select 2" } })}>again</button>
      <button onClick={() => navigate("/sql")}>plain</button>
    </div>
  );
}

function renderAt(state: unknown, onPayload: (p: { sql: string }) => void) {
  return render(
    <MemoryRouter initialEntries={[{ pathname: "/sql", state }]}>
      <Page onPayload={onPayload} />
    </MemoryRouter>,
  );
}

describe("useNavPayload", () => {
  it("handles the payload the page was opened with, once", () => {
    const seen = vi.fn();
    const { rerender } = renderAt({ sql: "select 1" }, seen);
    rerender(
      <MemoryRouter initialEntries={[{ pathname: "/sql", state: { sql: "select 1" } }]}>
        <Page onPayload={seen} />
      </MemoryRouter>,
    );
    expect(seen).toHaveBeenCalledTimes(1);
    expect(seen).toHaveBeenCalledWith({ sql: "select 1" });
  });

  it("handles a new payload while the page is already mounted", () => {
    const seen = vi.fn();
    renderAt(null, seen);
    expect(seen).not.toHaveBeenCalled();
    act(() => screen.getByText("again").click());
    expect(seen).toHaveBeenCalledWith({ sql: "select 2" });
  });

  it("removes a handled payload from the history entry", () => {
    renderAt({ sql: "select 1" }, vi.fn());
    expect(screen.getByTestId("state").textContent).toBe("null");
  });

  it("ignores a navigation that carries no payload", () => {
    const seen = vi.fn();
    renderAt(null, seen);
    act(() => screen.getByText("plain").click());
    expect(seen).not.toHaveBeenCalled();
  });
});
