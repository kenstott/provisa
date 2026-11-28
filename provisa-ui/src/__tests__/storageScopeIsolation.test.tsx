// Copyright (c) 2026 Kenneth Stott
// Canary: 93e3fb9e-b1de-4c18-93e6-2e969dd106a7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2008: each demo, instance and org keeps its own UI state. Switching to another carries
// nothing across; coming back restores what was left there.

import { afterEach, describe, expect, it, vi } from "vitest";
import { act, render, screen } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { useEffect, useState } from "react";
import { restoreCheckedDomains } from "../context/DomainFilterContext";
import { emptyTab, loadTabsMeta, persistTabsMeta } from "../pages/sql/tabHelpers";
import { bindStorageScope, getStored, setStored, storageScope } from "../lib/browserStorage";
import { CHECKED_DOMAINS_KEY, ROLE_DOMAIN_KEYS } from "../lib/domainFilterKeys";

const auth = vi.hoisted(() => ({
  value: {
    controlPlaneId: "cp-a" as string | null,
    activeOrgId: "acme" as string | null,
    loading: false,
  },
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth.value }));

import { StorageScopeGate } from "../components/StorageScopeGate";

const PET_DEMO = { controlPlaneId: "cp-pets", orgId: "default" };
const WEBLOGS_DEMO = { controlPlaneId: "cp-weblogs", orgId: "default" };

afterEach(() => localStorage.clear());

describe("state saved per control plane and org", () => {
  it("keeps each demo's domain filter, and restores it on coming back", () => {
    bindStorageScope(PET_DEMO);
    restoreCheckedDomains(["pets", "ops"], ROLE_DOMAIN_KEYS);
    setStored(CHECKED_DOMAINS_KEY, JSON.stringify(["pets"])); // the user unchecks ops

    bindStorageScope(WEBLOGS_DEMO);
    // A demo never visited starts with every domain checked: nothing of the pet store's carries.
    expect([...restoreCheckedDomains(["shop", "ops"], ROLE_DOMAIN_KEYS)].sort()).toEqual([
      "ops",
      "shop",
    ]);

    bindStorageScope(PET_DEMO);
    // Back in the pet store, ops is still unchecked; a domain new since then is checked.
    expect([...restoreCheckedDomains(["pets", "ops", "vet"], ROLE_DOMAIN_KEYS)].sort()).toEqual([
      "pets",
      "vet",
    ]);
  });

  it("keeps each demo's saved query tabs, and restores them on coming back", () => {
    bindStorageScope(PET_DEMO);
    persistTabsMeta([emptyTab("t1", "Pets", "select * from pets")], "t1");

    bindStorageScope(WEBLOGS_DEMO);
    const fresh = loadTabsMeta();
    expect(fresh.tabs.map((t) => t.sqlText)).toEqual([""]);
    persistTabsMeta([emptyTab("t9", "Hits", "select * from hits")], "t9");

    bindStorageScope({ controlPlaneId: "cp-pets", orgId: "globex" });
    expect(loadTabsMeta().tabs.map((t) => t.sqlText)).toEqual([""]); // another org of the demo

    bindStorageScope(PET_DEMO);
    const back = loadTabsMeta();
    expect(back.tabs.map((t) => [t.title, t.sqlText])).toEqual([["Pets", "select * from pets"]]);
    expect(back.activeId).toBe("t1");
  });
});

/** A page that shows what it read at mount and saves a value when it unmounts. */
function Page() {
  const [read] = useState(() => getStored("note") ?? "none");
  const [scope] = useState(() => storageScope());
  useEffect(() => () => setStored("left", "yes"), []);
  return (
    <div data-testid="page">
      {scope?.controlPlaneId}:{read}
    </div>
  );
}

function renderGate() {
  return render(
    <MantineProvider>
      <StorageScopeGate>
        <Page />
      </StorageScopeGate>
    </MantineProvider>,
  );
}

describe("the scope gate", () => {
  afterEach(() => {
    auth.value = { controlPlaneId: "cp-a", activeOrgId: "acme", loading: false };
  });

  it("mounts the pages once the control plane is known, in its scope", async () => {
    auth.value = { controlPlaneId: null, activeOrgId: null, loading: true };
    bindStorageScope(null);
    const view = renderGate();
    expect(screen.getByTestId("page-loading")).toBeTruthy();
    expect(screen.queryByTestId("page")).toBeNull();

    localStorage.setItem("cp-a:acme:note", "pets");
    auth.value = { controlPlaneId: "cp-a", activeOrgId: "acme", loading: false };
    view.rerender(
      <MantineProvider>
        <StorageScopeGate>
          <Page />
        </StorageScopeGate>
      </MantineProvider>,
    );
    expect((await screen.findByTestId("page")).textContent).toBe("cp-a:pets");
  });

  it("mounts the pages again in the next scope, which reads its own state", async () => {
    localStorage.setItem("cp-a:acme:note", "pets");
    localStorage.setItem("cp-b:acme:note", "weblogs");
    const view = renderGate();
    expect((await screen.findByTestId("page")).textContent).toBe("cp-a:pets");

    auth.value = { controlPlaneId: "cp-b", activeOrgId: "acme", loading: false };
    await act(async () => {
      view.rerender(
        <MantineProvider>
          <StorageScopeGate>
            <Page />
          </StorageScopeGate>
        </MantineProvider>,
      );
    });
    expect((await screen.findByTestId("page")).textContent).toBe("cp-b:weblogs");
    expect(storageScope()).toEqual({ controlPlaneId: "cp-b", orgId: "acme" });
    // What the pages of the scope left saved on the way out is in that scope, not the next.
    expect(localStorage.getItem("cp-a:acme:left")).toBe("yes");
    expect(localStorage.getItem("cp-b:acme:left")).toBeNull();
  });

  it("keeps the pages through an identity refresh", async () => {
    const view = renderGate();
    const page = await screen.findByTestId("page");
    auth.value = { controlPlaneId: "cp-a", activeOrgId: "acme", loading: true };
    view.rerender(
      <MantineProvider>
        <StorageScopeGate>
          <Page />
        </StorageScopeGate>
      </MantineProvider>,
    );
    expect(screen.getByTestId("page")).toBe(page);
  });
});
