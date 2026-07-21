// Copyright (c) 2026 Kenneth Stott
// Canary: ffae9eeb-1b20-4ad6-aefb-82f3562a875c
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1174: the role form surfaces per-role rate + query-complexity limits and stages them through
// the upsertRole save path (Hasura api_limits parity, editable in the UI).

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../test-utils/render";
import { MemoryRouter } from "react-router-dom";

const upsertRoleSpy = vi.fn(async () => ({ success: true, message: "" }));

vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    setDomains: vi.fn(),
    setSelectedDomain: vi.fn(),
    checkedDomains: new Set<string>(),
  }),
}));

vi.mock("../../hooks/useAdminQueries", () => ({
  useRoles: () => ({ roles: [], loading: false, refetch: vi.fn() }),
  useRLSRules: () => ({ rlsRules: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useUpsertRole: () => ({ upsertRole: upsertRoleSpy, loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: vi.fn(), loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: vi.fn(), loading: false }),
  // Keep the module mock complete so it can't leak an undefined hook into other tests.
  useMaterializeStoreInfo: () => ({
    materializeStoreInfo: null,
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
}));

import { SecurityPage } from "../SecurityPage";

function renderPage() {
  return render(
    <MemoryRouter>
      <SecurityPage />
    </MemoryRouter>,
  );
}

// Mantine spreads data-testid onto the underlying <input>, so the testid IS the input element.
const input = (testid: string) => screen.getByTestId(testid) as HTMLInputElement;

describe("SecurityPage — per-role rate & query-complexity limits (REQ-1174)", () => {
  it("renders the limit fields when the role form is open", () => {
    renderPage();
    fireEvent.click(screen.getByTestId("toggle-role-form"));
    expect(screen.getByTestId("role-req-per-sec")).toBeInTheDocument();
    expect(screen.getByTestId("role-max-depth")).toBeInTheDocument();
    expect(screen.getByTestId("role-max-nodes")).toBeInTheDocument();
    expect(screen.getByTestId("role-max-time-ms")).toBeInTheDocument();
  });

  it("stages the entered limits through upsertRole", async () => {
    upsertRoleSpy.mockClear();
    renderPage();
    fireEvent.click(screen.getByTestId("toggle-role-form"));

    fireEvent.change(input("role-id-input"), { target: { value: "analyst" } });
    fireEvent.change(input("role-req-per-sec"), { target: { value: "5" } });
    fireEvent.change(input("role-max-depth"), { target: { value: "6" } });
    fireEvent.change(input("role-max-nodes"), { target: { value: "200" } });
    fireEvent.change(input("role-max-time-ms"), { target: { value: "3000" } });

    fireEvent.click(screen.getByTestId("save-role"));

    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalledTimes(1));
    expect(upsertRoleSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "analyst",
        rateLimit: {
          requestsPerSecond: 5,
          maxQueryDepth: 6,
          maxQueryNodes: 200,
          maxQueryTimeMs: 3000,
        },
      }),
    );
  });

  it("sends rateLimit=null when no limit is entered", async () => {
    upsertRoleSpy.mockClear();
    renderPage();
    fireEvent.click(screen.getByTestId("toggle-role-form"));
    fireEvent.change(input("role-id-input"), { target: { value: "guest" } });
    fireEvent.click(screen.getByTestId("save-role"));
    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalledTimes(1));
    expect(upsertRoleSpy).toHaveBeenCalledWith(expect.objectContaining({ rateLimit: null }));
  });
});
