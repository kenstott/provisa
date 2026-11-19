// Copyright (c) 2026 Kenneth Stott
// Canary: 66082f79-532c-4314-a0db-dd66449bf623
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: a link to another page is drawn only when the viewer holds that page's right.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../test-utils/render";

const auth = vi.hoisted(() => ({ capabilities: [] as string[] }));
vi.mock("../../context/AuthContext", () => ({ useAuth: () => auth }));

import { EntityLink } from "../EntityLink";

describe("EntityLink", () => {
  it("links a term for a viewer who reads the glossary", () => {
    auth.capabilities = ["glossary_read"];
    render(
      <EntityLink kind="term" id={7} testId="t">
        customer
      </EntityLink>,
    );
    expect(screen.getByTestId("t")).toHaveAttribute("href", "/context/ontology?term=7");
  });

  it("shows the name as plain text to a viewer without the page's right", () => {
    auth.capabilities = ["query_development"];
    render(
      <EntityLink kind="metric" id="revenue" testId="m">
        revenue
      </EntityLink>,
    );
    expect(screen.getByTestId("m")).not.toHaveAttribute("href");
    expect(screen.getByTestId("m")).toHaveTextContent("revenue");
    expect(screen.queryByRole("link")).toBeNull();
  });
});
