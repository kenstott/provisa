// Copyright (c) 2026 Kenneth Stott
// Canary: c106dfad-b01f-470f-94c7-180ca403706e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, expect, it, vi } from "vitest";
import { Link, Route, Routes } from "react-router-dom";
import { fireEvent, render, screen } from "../../test-utils/render";
import { PageErrorBoundary } from "../PageErrorBoundary";

function Broken(): never {
  throw new Error("the page's own failure");
}

describe("a page that throws while rendering", () => {
  it("says so in its place, with its error, and the next page renders", async () => {
    const quiet = vi.spyOn(console, "error").mockImplementation(() => undefined);
    render(
      <>
        <nav>
          <Link to="/fine">fine</Link>
        </nav>
        <PageErrorBoundary>
          <Routes>
            <Route path="/broken" element={<Broken />} />
            <Route path="/fine" element={<p>the next page</p>} />
          </Routes>
        </PageErrorBoundary>
      </>,
      { initialEntries: ["/broken"] },
    );
    expect(screen.getByTestId("page-error-message")).toHaveTextContent("the page's own failure");
    // The rest of the app is still there, and leaving the page renders the next one.
    fireEvent.click(screen.getByText("fine"));
    expect(await screen.findByText("the next page")).toBeInTheDocument();
    expect(screen.queryByTestId("page-error")).toBeNull();
    quiet.mockRestore();
  });
});
