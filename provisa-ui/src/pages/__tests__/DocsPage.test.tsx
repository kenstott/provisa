// Copyright (c) 2026 Kenneth Stott
// Canary: 354b6870-5ffa-400b-b759-866f82f2b9f8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.


// REQ-1984 (amended 2026-11-24): /docs?page=<page>#<section> frames the docs site at that page and
// section, online or offline, and /docs alone frames the site's home.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../test-utils/render";
import { DocsPage } from "../DocsPage";

beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(null, { status: 200 })),
  );
});
afterEach(() => vi.unstubAllGlobals());

const frameSrc = async () => {
  const frame = await screen.findByTestId("docs-frame");
  return frame.getAttribute("src");
};

describe("the Docs page", () => {
  it("frames the site's home when no page is named", async () => {
    render(<DocsPage />, { initialEntries: ["/docs"] });
    await waitFor(async () => expect(await frameSrc()).toBe("https://provisa.dev/docs/"));
  });

  it("frames the page and section the URL names", async () => {
    render(<DocsPage />, { initialEntries: ["/docs?page=code-sources/#python-reference"] });
    await waitFor(async () =>
      expect(await frameSrc()).toBe("https://provisa.dev/docs/code-sources/#python-reference"),
    );
  });

  it("keeps the page and section on the offline copy", async () => {
    render(<DocsPage />, { initialEntries: ["/docs?page=code-sources/#spark-reference"] });
    await screen.findByTestId("docs-frame");
    fireEvent.click(screen.getByTestId("docs-toggle-source"));
    await waitFor(async () =>
      expect(await frameSrc()).toBe("/docs-site/code-sources/#spark-reference"),
    );
  });
});
