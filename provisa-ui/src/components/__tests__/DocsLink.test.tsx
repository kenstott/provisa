// Copyright (c) 2026 Kenneth Stott
// Canary: 9c3f1e52-7a64-4b0d-8e21-5d6a0b47f3c8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Maintainer 2026-11-28: every link to the documentation opens in a new browser tab.

import { describe, expect, it } from "vitest";
import ReactMarkdown from "react-markdown";
import { render, screen } from "../../test-utils/render";
import { DocsLink, MarkdownLink } from "../DocsLink";
import { isDocsHref } from "../docsTarget";

function expectNewTab(link: HTMLElement) {
  expect(link.tagName).toBe("A");
  expect(link).toHaveAttribute("target", "_blank");
  expect(link).toHaveAttribute("rel", "noopener noreferrer");
}

describe("DocsLink", () => {
  it("opens a docs target in the app's Docs page, in a new tab", () => {
    render(<DocsLink target="configuration/#tables" testId="d" />);
    const link = screen.getByTestId("d");
    expect(link).toHaveAttribute("href", "/docs?page=configuration/#tables");
    expect(link).toHaveTextContent("Read the documentation");
    expectNewTab(link);
  });

  it("opens a docs address as given, with its own text, in a new tab", () => {
    render(
      <DocsLink href="/docs-site/ontology/" testId="d">
        Guide
      </DocsLink>,
    );
    const link = screen.getByTestId("d");
    expect(link).toHaveAttribute("href", "/docs-site/ontology/");
    expect(link).toHaveTextContent("Guide");
    expectNewTab(link);
  });
});

describe("a link in rendered Markdown", () => {
  it("opens a docs link in a new tab", () => {
    render(
      <ReactMarkdown components={{ a: MarkdownLink }}>
        {"See [the guide](/docs?page=sources/) and [tables](/tables)."}
      </ReactMarkdown>,
    );
    const docs = screen.getByRole("link", { name: "the guide" });
    expect(docs).toHaveAttribute("href", "/docs?page=sources/");
    expectNewTab(docs);
    const other = screen.getByRole("link", { name: "tables" });
    expect(other).toHaveAttribute("href", "/tables");
    expect(other).not.toHaveAttribute("target");
  });
});

describe("isDocsHref", () => {
  it.each(["/docs", "/docs?page=sources/", "/docs-site/ontology/#x", "https://provisa.dev/docs/"])(
    "%s is the documentation",
    (href) => expect(isDocsHref(href)).toBe(true),
  );

  it.each(["/tables", "/docsify", "https://example.com/docs/", "docs/"])("%s is not", (href) =>
    expect(isDocsHref(href)).toBe(false),
  );
});
