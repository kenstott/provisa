// Copyright (c) 2026 Kenneth Stott
// Canary: 8c2e5a70-1d49-4b63-a8f7-3e6b9d0c4f15
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1960: Wikipedia is in the source pick list, carried by the File Crawler; its setup names
// the pages, offers "See also" as a choice, and shows every crawl setting with its default.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { BRAND_AUTH, BRAND_CARRIER, SOURCE_TYPES } from "../constants";
import { SourceFormFieldsExtended } from "../SourceFormFieldsExtended";
import type { SourceFormFieldsProps } from "../SourceFormFields";
import { backendType, sourceBrand } from "../sourceHelpers";
import { WikipediaFields } from "../WikipediaFields";
import {
  WIKIPEDIA_CRAWL_FIELDS,
  wikipediaFieldsFromHints,
  wikipediaHints,
  wikipediaMissing,
} from "../wikipedia";

describe("the source pick list", () => {
  it("offers Wikipedia as a type the File Crawler carries", () => {
    expect(SOURCE_TYPES.find((s) => s.value === "wikipedia")).toMatchObject({ label: "Wikipedia" });
    expect(BRAND_CARRIER.wikipedia).toBe("files");
    expect(backendType("wikipedia")).toBe("files");
  });
});

describe("what a Wikipedia setup saves", () => {
  it("is the brand and only what was chosen", () => {
    const hints = wikipediaHints({ wp_pages: "List of tallest buildings\n\n  Dubai  " });
    expect(hints).toEqual({
      brand: "wikipedia",
      wikipedia: { pages: ["List of tallest buildings", "Dubai"], follow_see_also: false },
    });
    expect(sourceBrand(JSON.stringify(hints))).toBe("wikipedia");
  });

  it("carries the choices and any crawl setting stated in place of a default", () => {
    const fields = {
      wp_pages: "Dubai",
      wp_language: "de",
      wp_max_depth: "0",
      wp_max_pages: "5",
      wp_contact: "ops@example.test",
      wp_follow_see_also: "true",
      wpc_table_selector: "table",
      wpc_html_table_min_rows: "3",
      wpc_remove_selectors: ".navbox\n.infobox",
    };
    const hints = wikipediaHints(fields);
    expect(hints.wikipedia).toEqual({
      pages: ["Dubai"],
      follow_see_also: true,
      language: "de",
      contact: "ops@example.test",
      max_depth: 0,
      max_pages: 5,
      crawl: {
        table_selector: "table",
        html_table_min_rows: 3,
        remove_selectors: [".navbox", ".infobox"],
      },
    });
    // The form that edits the source shows what was saved.
    expect(wikipediaFieldsFromHints(JSON.stringify(hints))).toEqual(fields);
  });
});

describe("what a Wikipedia setup still needs", () => {
  it("is a page, and the See also heading of an edition Provisa does not know", () => {
    expect(wikipediaMissing({})).toEqual(["wp_pages"]);
    expect(wikipediaMissing({ wp_pages: "Dubai" })).toEqual([]);
    expect(wikipediaMissing({ wp_pages: "Dubaï", wp_language: "fr" })).toEqual([
      "wp_see_also_heading",
    ]);
    expect(
      wikipediaMissing({ wp_pages: "Dubaï", wp_language: "fr", wp_follow_see_also: "true" }),
    ).toEqual([]);
    expect(
      wikipediaMissing({ wp_pages: "Dubaï", wp_language: "fr", wp_see_also_heading: "Articles_connexes" }),
    ).toEqual([]);
  });
});

describe("the Wikipedia setup form", () => {
  it("has every crawl setting, and asks what See also is called only of an edition it cannot name", () => {
    const setFields = vi.fn();
    const { rerender } = render(<WikipediaFields fields={{}} setFields={setFields} />);
    expect(screen.getByTestId("wikipedia-wp_pages")).toBeInTheDocument();
    for (const f of WIKIPEDIA_CRAWL_FIELDS) {
      expect(screen.getByTestId(`wikipedia-wpc_${f.key}`)).toBeInTheDocument();
    }
    // English is an edition Provisa can name the section of: nothing is asked.
    expect(screen.queryByTestId("wikipedia-wp_see_also_heading")).not.toBeInTheDocument();
    rerender(<WikipediaFields fields={{ wp_language: "fr" }} setFields={setFields} />);
    expect(screen.getByTestId("wikipedia-wp_see_also_heading")).toBeInTheDocument();
    expect(screen.getByText(/as it appears on a page/)).toBeInTheDocument();
    rerender(
      <WikipediaFields fields={{ wp_language: "fr", wp_follow_see_also: "true" }} setFields={setFields} />,
    );
    expect(screen.queryByTestId("wikipedia-wp_see_also_heading")).not.toBeInTheDocument();
  });
});

describe("the Wikipedia setup in the Sources form", () => {
  const form = (type: string) =>
    ({
      // The Sources page's empty form (SourcesPage.tsx), with the type under test.
      form: {
        id: "s",
        type,
        host: "",
        port: 0,
        database: "",
        username: "",
        password: "",
        gqlNamingConvention: "",
        cacheTtl: "",
        cacheEnabled: true,
        replicate: null,
        region: null,
        loadProtected: false,
        offPeakWindow: "",
        offPeakTz: "UTC",
        changeSignal: "ttl",
        sentinelPath: "",
        freshnessGate: false,
        maxLiveConcurrency: "",
        path: "",
        description: "",
      },
      setForm: vi.fn(),
      authType: "none",
      setAuthType: vi.fn(),
      authFields: {},
      setAuthFields: vi.fn(),
      domains: [],
    }) as unknown as SourceFormFieldsProps;

  it("asks for no credential: Wikipedia is read without one", () => {
    expect(BRAND_AUTH.wikipedia).toBe("none");
    render(<SourceFormFieldsExtended {...form("wikipedia")} />);
    expect(screen.getByTestId("wikipedia-wp_pages")).toBeTruthy();
    expect(screen.queryByTestId("brand-token-input")).toBeNull();
    expect(screen.queryByLabelText(/token|password|secret|api key/i)).toBeNull();
  });

  it("gives no brand a credential by default: each states its own", () => {
    expect(Object.keys(BRAND_AUTH).sort()).toEqual(Object.keys(BRAND_CARRIER).sort());
    render(<SourceFormFieldsExtended {...form("a_brand_that_states_nothing")} />);
    expect(screen.queryByTestId("brand-token-input")).toBeNull();
  });

  it("asks a brand that states a token for it", () => {
    expect(BRAND_AUTH.github).toBe("token");
    render(<SourceFormFieldsExtended {...form("github")} />);
    expect(screen.getByTestId("brand-token-input")).toBeTruthy();
  });
});
