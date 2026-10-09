// Copyright (c) 2026 Kenneth Stott
// Canary: c6cb20dc-f9da-4065-9e10-266a8a0169c0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-540, REQ-541: an AskAmerica source's schema list is built from the subjects the SERVER
// offers. The form holds no subject or schema of its own.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import {
  schemasForSubjects,
  subjectsOfSchemas,
  type GovdataSubjectCatalog,
} from "../govdataSubjects";

// What the server answers with, in shape; the names are this test's own.
const CATALOG: GovdataSubjectCatalog = {
  subjects: [
    { value: "MARKETS", label: "Markets", schemas: ["filings", "futures"] },
    { value: "PEOPLE", label: "People", schemas: ["headcount"] },
    { value: "SCHOOLS", label: "Schools", schemas: ["headcount", "campuses"] },
  ],
  linkerSchemas: ["links", "places"],
};

describe("the schemas a source is saved with", () => {
  it("are its subjects' schemas, then the linker schemas, once each", () => {
    expect(schemasForSubjects(CATALOG, ["MARKETS"])).toEqual([
      "filings",
      "futures",
      "links",
      "places",
    ]);
    expect(schemasForSubjects(CATALOG, ["PEOPLE", "SCHOOLS"])).toEqual([
      "headcount",
      "campuses",
      "links",
      "places",
    ]);
  });

  it("are the linker schemas alone when no subject is ticked", () => {
    expect(schemasForSubjects(CATALOG, [])).toEqual(["links", "places"]);
  });

  it("refuse a subject the server does not offer, by name", () => {
    expect(() => schemasForSubjects(CATALOG, ["MARKETS", "MOON"])).toThrow(
      "Unknown AskAmerica subject: MOON",
    );
  });
});

describe("the subjects an existing source shows as ticked", () => {
  it("are those one of whose own schemas is stored", () => {
    expect(subjectsOfSchemas(CATALOG, ["futures", "links", "places"])).toEqual(["MARKETS"]);
    expect(subjectsOfSchemas(CATALOG, ["headcount"])).toEqual(["PEOPLE", "SCHOOLS"]);
  });

  it("are none for a source that stores only the linker schemas", () => {
    expect(subjectsOfSchemas(CATALOG, ["links", "places"])).toEqual([]);
  });

  it("round-trip: what is saved for a choice reads back as that choice", () => {
    for (const choice of [["MARKETS"], ["SCHOOLS", "MARKETS"], []]) {
      const saved = schemasForSubjects(CATALOG, choice);
      expect(new Set(subjectsOfSchemas(CATALOG, saved))).toEqual(
        // SCHOOLS brings "headcount", which PEOPLE also owns: a shared schema ticks both.
        new Set(choice.includes("SCHOOLS") ? [...choice, "PEOPLE"] : choice),
      );
    }
  });
});

describe("the form's sources", () => {
  const read = (path: string) => readFileSync(resolve(__dirname, "..", "..", path), "utf8");

  it("keep no subject list and append no schema by hand", () => {
    expect(read("sources/constants.ts")).not.toContain("GOVDATA_SUBJECTS");
    const page = read("SourcesPage.tsx");
    expect(page).not.toContain("GOVDATA_SUBJECTS");
    expect(page).not.toMatch(/"ref",\s*"geo"/);
    expect(page).toContain("schemasForSubjects(requireGovdataCatalog(), govdataSubjects)");
  });

  it("ask the server for the subjects", () => {
    const query = readFileSync(resolve(__dirname, "..", "..", "..", "hooks/admin.graphql"), "utf8");
    expect(query).toMatch(/query GovdataSubjectsQuery \{\s*govdataSubjects \{/);
    expect(query).toContain("linkerSchemas");
  });
});
