// Copyright (c) 2026 Kenneth Stott
// Canary: 9029ba5b-4ee1-4d24-a4cc-03a3e8fbdd4c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.


// REQ-1984 (amended 2026-11-24): a code source's dialog links the reference of the language chosen,
// the link changes with the language, and it opens the in-app Docs page at that language's section.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ activeOrgId: "acme" }) }));
import { Route, Routes } from "react-router-dom";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import { FormLayout } from "../../../components/form/FormLayout";
import { sourceFormProps, sourceOf } from "../../../test-utils/sourceForm";
import { DocsPage } from "../../DocsPage";
import { CodeReferenceLink } from "../CodeReferenceLink";
import { SourceFormFields } from "../SourceFormFields";
import { codeReferenceHref } from "../codeSource";

const LANGUAGES = [
  ["node", "Node.js", "node-reference"],
  ["python", "Python", "python-reference"],
  ["go", "Go", "go-reference"],
  ["rust", "Rust", "rust-reference"],
  ["spark", "Spark", "spark-reference"],
] as const;

beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response("[]", { status: 200 })),
  );
});
afterEach(() => vi.unstubAllGlobals());

const form = (type: string) => (
  <FormLayout>
    <SourceFormFields {...sourceFormProps(sourceOf(type))} />
  </FormLayout>
);

describe("a code source's reference link", () => {
  it.each(LANGUAGES)("on a %s source opens %s's reference section", (type, label, anchor) => {
    render(form(type));
    const link = screen.getByTestId("code-reference-link");
    expect(link.getAttribute("href")).toBe(`/docs?page=code-sources/#${anchor}`);
    expect(link.textContent).toBe(`${label} reference`);
  });

  it("changes with the language chosen", () => {
    const { rerender } = render(form("python"));
    expect(screen.getByTestId("code-reference-link").getAttribute("href")).toBe(
      "/docs?page=code-sources/#python-reference",
    );
    rerender(form("rust"));
    const link = screen.getByTestId("code-reference-link");
    expect(link.getAttribute("href")).toBe("/docs?page=code-sources/#rust-reference");
    expect(link.textContent).toBe("Rust reference");
  });

  it("is not on a source that is not code", () => {
    render(form("postgresql"));
    expect(screen.queryByTestId("code-reference-link")).toBeNull();
  });

  it("refuses a type that is not a code language by name", () => {
    expect(() => codeReferenceHref("postgresql")).toThrow("postgresql is not a code source type");
  });

  it("opens the Docs page framed at that section", async () => {
    render(
      <Routes>
        <Route
          path="/sources"
          element={
            <FormLayout>
              <CodeReferenceLink type="go" />
            </FormLayout>
          }
        />
        <Route path="/docs" element={<DocsPage />} />
      </Routes>,
      { initialEntries: ["/sources"] },
    );
    fireEvent.click(screen.getByTestId("code-reference-link"));
    const frame = await screen.findByTestId("docs-frame");
    await waitFor(() =>
      expect(frame.getAttribute("src")).toBe("https://provisa.dev/docs/code-sources/#go-reference"),
    );
  });
});
