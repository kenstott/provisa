// Copyright (c) 2026 Kenneth Stott
// Canary: 4a378dff-acbe-4b2a-a041-30d8609fd33f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2193: the OData page gives the service root and the role's $metadata, each to copy.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../test-utils/render";
import i18n from "../i18n";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ actingRoles: "analyst" }),
}));

import { ODataPage } from "../pages/ODataPage";
import { NAV_GROUPS } from "../components/navGroups";

const t = i18n.getFixedT("en");
const ROOT = `${window.location.origin}/data/odata`;

describe("ODataPage", () => {
  // The viewer below the addresses reads the role's $metadata: here, a model with no entity set.
  beforeEach(() => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response('<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx"/>'),
      ),
    );
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("shows the service root and $metadata addresses", () => {
    render(<ODataPage />);
    expect(screen.getByTestId("odata-service-root")).toHaveTextContent(ROOT);
    expect(screen.getByTestId("odata-metadata")).toHaveTextContent(`${ROOT}/$metadata`);
  });

  it("copies the service root", () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    render(<ODataPage />);
    fireEvent.click(screen.getAllByRole("button", { name: t("odataPage.copyUrl") })[0]);
    expect(writeText).toHaveBeenCalledWith(ROOT);
  });

  it("is on the Explore menu beside JSON:API", () => {
    const explore = NAV_GROUPS.find((g) => g.id === "explore")!;
    const routes = explore.items.map((i) => i.to);
    expect(routes.indexOf("/odata")).toBe(routes.indexOf("/jsonapi") + 1);
  });
});
