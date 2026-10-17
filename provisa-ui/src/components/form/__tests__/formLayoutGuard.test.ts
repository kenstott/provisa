// Copyright (c) 2026 Kenneth Stott
// Canary: ea4f6418-79a5-4c05-979a-1b1f64211fe5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1968: every form is laid out by the shared layout (components/form/FormLayout.tsx). This
// guard reads every component's TSX and fails on a single-line field stretched to full width, a
// form laying its fields out outside the shared layout, action buttons aligned anywhere but the
// start, and a button stretched by `fullWidth` or by its container.

import { describe, it, expect } from "vitest";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { checkFormRules, type FormViolation } from "./formRules";

const SRC = resolve(__dirname, "../../..");

/**
 * Permanent: files whose controls are not a form. Each is exempt from one rule only (a control
 * outside a FormField) and says why. Buttons and action rows in these files are still held.
 */
const NOT_A_FORM: Readonly<Record<string, string>> = {
  "components/admin/FilterInput.tsx": "the search box above a list",
  "components/admin/MergeRequestsPanel.tsx":
    "the open-only filter above the list of merge requests",
  "components/admin/RegionSelect.tsx": "one reusable control, placed by the form that uses it",
  "components/admin/RepoBrowser.tsx": "the pickers of what the repository browser shows",
  "components/admin/ReplicateSelect.tsx": "one reusable control, placed by the form that uses it",
  "components/RegionSelector.tsx": "the region switch in the page header",
  "components/GrantPicker.tsx":
    "one reusable control (a grant list and its combine popover), placed by the form that uses it",
  "components/MaskListEditor.tsx":
    "one reusable control (a column's ordered mask list), placed by the form that uses it",
  "pages/tables/DescriptionField.tsx": "one reusable control, placed by the form that uses it",
  "pages/tables/VectorModelCell.tsx": "one reusable control, placed by the form that uses it",
  "pages/tables/NumericSizeInputs.tsx": "one reusable control, placed by the form that uses it",
  "components/GovernedTableViewer.tsx": "the filter above a data preview",
  "components/NavBar.tsx": "the do-not-ask-again tick of the sign-out dialog",
  "components/SourceTypePicker.tsx": "the search box of the source-type picker",
  "components/graph/canvas/CanvasControls.tsx": "a slider on the graph canvas",
  "components/graph/GraphFrame.tsx": "the graph toolbar",
  "components/graph/GraphSidebar.tsx": "renaming a favourite in place in the sidebar list",
  "components/EditorTabStrip.tsx": "renaming an editor tab in place in the tab strip",
  "components/erd/ErdPanel.tsx": "the diagram toolbar",
  "components/sql-modeling/JoinCanvas.tsx": "a join picker on the modeling canvas",
  "components/SqlModelingModal.tsx": "the toolbar of the modeling workbench",
  "pages/sql/ColumnFilterMenu.tsx": "the filter popover of a result column",
  "pages/sql/JoinCanvas.tsx": "join pickers on the SQL canvas",
  "pages/sql/ResultsGrid.tsx": "paging and filter controls of the result grid",
  "pages/sql/SqlEditorPanel.tsx": "the SQL editor toolbar",
  "pages/SqlPage.tsx": "the SQL page toolbar",
  "pages/QueryPage.tsx": "the query explorer toolbar",
  "pages/tables/TableReadView.tsx": "the Draft tick among a table detail's actions",
  "pages/NlPage.tsx": "the question box of the natural-language page",
  "pages/GrpcPage.tsx": "the request builder of the gRPC explorer",
  "pages/JsonApiPage.tsx": "the request builder of the JSON:API explorer",
  "pages/jsonapi/IncludeTree.tsx": "the include tree of the JSON:API request builder",
  "pages/jsonapi/ParamsSection.tsx": "the parameter inputs of the JSON:API request builder",
  "pages/McpExplorePage.tsx": "the address bar of the MCP explorer",
  "pages/LineagePage.tsx": "the role filter above the lineage graph",
  "plugins/provisa-tools.tsx": "controls of the GraphiQL tools plugin",
  "pages/tables/ProfileChecksBar.tsx": "the action bar above the profile results",
  "pages/tables/ProfilerPanel.tsx": "the add-metric picker above the profile results",
};

/**
 * Permanent: surfaces that edit something in place, in a popover or a row. Their controls are not
 * a form (as NOT_A_FORM), and their compact Save/Apply and Cancel are not the shared footer's.
 * Everything else is still held: a full-size Save or Cancel, a stretched button, an action row.
 */
const IN_PLACE_EDIT: Readonly<Record<string, string>> = {
  "pages/sources/ServiceNowApiList.tsx":
    "an API of a ServiceNow instance switched on or off in place in its row",
  "components/ChatPanel.tsx":
    "Polly's composer and the answer boxes of a question it asks, in the conversation itself",
  "components/admin/GlossaryRelationships.tsx":
    "a term's relationship picked in the row that adds it, in the term's detail",
  "components/admin/GlossaryTab.tsx":
    "a term's name, definition and export tick each saved in place in its detail; the list's filter ticks",
  "components/TagControl.tsx": "a tag assigned and edited in its popover, beside what it tags",
  "pages/tables/ProfileConstraintsModal.tsx":
    "a column's constraints edited in the row of the checklist that lists them",
};

/**
 * Permanent: parts of an explorer that no form rule is applied to. The explorers (query, graph,
 * SQL, JSON:API, OpenAPI, MCP, lineage, and what they alone mount) are a different experience
 * from the forms; these files stay as they are.
 */
const EXPLORER_REASON =
  "an explorer: a different experience, outside the form rules (maintainer, 2026-10-10)";
const EXPLORERS: Readonly<Record<string, string>> = {
  "components/graph/Inspector.tsx": EXPLORER_REASON,
  "components/graph/Neo4jExportModal.tsx": EXPLORER_REASON,
  "components/graph/graph-context-menus.tsx": EXPLORER_REASON,
  "pages/sql/ViewModal.tsx": EXPLORER_REASON,
};

/**
 * Permanent: buttons that may fill their container, by file and a marker in the button's own JSX.
 */
const STRETCHED_BUTTONS: Readonly<Record<string, { marker: string; reason: string }[]>> = {
  "components/OrgSelectionPrompt.tsx": [
    { marker: "org-selection-option-", reason: "a list row that is itself the action" },
  ],
  "components/OrgSelector.tsx": [
    { marker: "org-selector-option-", reason: "a list row that is itself the action" },
  ],
  "pages/LoginPage.tsx": [
    {
      marker: "saml-signin-button",
      reason: "the sign-in card's one primary action: a way to sign in, the card's width",
    },
    {
      marker: "firebase-signin-button",
      reason: "an identity provider of the sign-in card: a peer way to sign in",
    },
    {
      marker: "firebase-signin-github-button",
      reason: "an identity provider of the sign-in card: a peer way to sign in",
    },
    {
      marker: "firebase-signin-microsoft-button",
      reason: "an identity provider of the sign-in card: a peer way to sign in",
    },
    {
      marker: "firebase-email-submit",
      reason: "the single call to action of the sign-in card's email form, the card's width",
    },
    {
      marker: "firebase-email-disclosure",
      reason: "the sign-in card's email method, beside its providers: a peer way to sign in",
    },
  ],
};

/**
 * Permanent: the file that IS the shared layout. Its footer is where the form's one Save and one
 * Cancel are defined, which is the very thing the rules send every other file to use, so the
 * rules are not read against it. Its own behaviour is held by FormLayout.test.tsx.
 */
const THE_SHARED_LAYOUT = "components/form/FormLayout.tsx";

/**
 * Shrinking: files that do not follow the rules yet. A file leaves this list in the commit that
 * converts it, and the guard fails a listed file that already passes. Nothing is added here,
 * except when the rules themselves grow: the files then caught are listed in that same commit.
 */
const NOT_YET_CONVERTED: readonly string[] = [];

function componentFiles(dir: string, found: string[] = []): string[] {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) {
      if (name !== "__tests__" && name !== "__mocks__" && name !== "test-utils") {
        componentFiles(path, found);
      }
    } else if (name.endsWith(".tsx") && !name.includes(".test.")) {
      found.push(relative(SRC, path));
    }
  }
  return found;
}

/** The English catalog: the core file and every per-component file, as the app merges them. */
const english: Record<string, unknown> = (() => {
  const dir = join(SRC, "i18n/locales");
  const merged = JSON.parse(readFileSync(join(dir, "en.json"), "utf8")) as Record<string, unknown>;
  for (const name of readdirSync(join(dir, "en"))) {
    if (name.endsWith(".json") && !name.startsWith(".")) {
      Object.assign(merged, JSON.parse(readFileSync(join(dir, "en", name), "utf8")));
    }
  }
  return merged;
})();

function englishOf(key: string): string | undefined {
  let node: unknown = english;
  for (const part of key.split(".")) {
    if (typeof node !== "object" || node === null) return undefined;
    node = (node as Record<string, unknown>)[part];
  }
  return typeof node === "string" ? node : undefined;
}

function violationsOf(
  file: string,
  notAForm = file in NOT_A_FORM,
  inPlaceEdit = file in IN_PLACE_EDIT,
): FormViolation[] {
  return checkFormRules(file, readFileSync(join(SRC, file), "utf8"), {
    notAForm,
    inPlaceEdit,
    textOf: englishOf,
    stretchedButtonExceptions: (STRETCHED_BUTTONS[file] ?? []).map((e) => e.marker),
  });
}

function describeViolations(file: string, violations: FormViolation[]): string {
  return violations.map((v) => `${file}:${v.line} ${v.rule}: ${v.detail}`).join("\n");
}

const rules = (source: string, options = {}) =>
  checkFormRules("Sample.tsx", source, options).map((v) => v.rule);

describe("the form rules fail what they are meant to fail", () => {
  it("a single-line field stretched to full width", () => {
    expect(
      rules(
        `<FormLayout><FormField size="full"><TextInput label="Host" /></FormField></FormLayout>`,
      ),
    ).toEqual(["full-width-single-line"]);
    expect(
      rules(
        `<FormLayout><FormField size="long"><Select style={{ gridColumn: "1 / -1" }} /></FormField></FormLayout>`,
      ),
    ).toEqual(["full-width-single-line"]);
    expect(
      rules(
        `const full = { gridColumn: "1 / -1" };
         const x = <FormLayout><FormField size="long"><NumberInput style={full} /></FormField></FormLayout>;`,
      ),
    ).toEqual(["full-width-single-line"]);
  });

  it("a multi-line field may be full width", () => {
    expect(
      rules(
        `<FormLayout><FormField size="full"><Textarea label="Notes" /></FormField></FormLayout>`,
      ),
    ).toEqual([]);
  });

  it("a form laying its fields out outside the shared layout", () => {
    expect(rules(`<Stack><TextInput label="Name" /></Stack>`)).toEqual(["outside-shared-layout"]);
    expect(rules(`<form className="form-card"></form>`)).toEqual(["outside-shared-layout"]);
    expect(rules(`<div style={{ gridColumn: "1 / -1" }}>x</div>`)).toEqual([
      "outside-shared-layout",
    ]);
    expect(
      rules(
        `<FormLayout><SimpleGrid cols={2}><FormField size="short"><NumberInput /></FormField></SimpleGrid></FormLayout>`,
      ),
      // the grid's own columns, and the field it lays out
    ).toEqual(["outside-shared-layout", "outside-shared-layout"]);
    expect(rules(`<SimpleGrid cols={3}><Card /><Card /></SimpleGrid>`)).toEqual([]);
    expect(
      rules(
        `<FormLayout component="form" className="form-card"><FormField size="short"><NumberInput /></FormField></FormLayout>`,
      ),
    ).toEqual([]);
  });

  it("a control in a table cell, or in a file that is not a form, is not a form field", () => {
    expect(rules(`<Table.Td><TextInput aria-label="Alias" /></Table.Td>`)).toEqual([]);
    expect(rules(`<Group><TextInput type="search" /></Group>`, { notAForm: true })).toEqual([]);
  });

  it("action buttons aligned anywhere but the start", () => {
    expect(rules(`<Group justify="flex-end"><Button>Run</Button></Group>`)).toEqual([
      "actions-not-at-start",
    ]);
    expect(
      rules(`<Group justify="space-between"><Button>Close</Button><Button>Run</Button></Group>`),
    ).toEqual(["actions-not-at-start"]);
    expect(rules(`<div className="modal-actions"><Button>Run</Button></div>`)).toEqual([
      "actions-not-at-start",
    ]);
    expect(
      rules(
        `<div style={{ display: "flex", justifyContent: "flex-end" }}>{ok && <Button>Run</Button>}</div>`,
      ),
    ).toEqual(["actions-not-at-start"]);
  });

  it("a title with a button at the far end, and a footer, are not misaligned actions", () => {
    expect(
      rules(
        `<Group justify="space-between"><Title>Sources</Title><Button>+ Source</Button></Group>`,
      ),
    ).toEqual([]);
    expect(rules(`<FormFooter><Button>Run</Button><Button>Close</Button></FormFooter>`)).toEqual(
      [],
    );
    expect(rules(`<Group justify="flex-end"><ActionIcon /></Group>`)).toEqual([]);
  });

  it("a button stretched by fullWidth or by its container", () => {
    expect(rules(`<Group><Button fullWidth>Run</Button></Group>`)).toEqual(["button-stretched"]);
    expect(rules(`<Stack><Button>Download</Button></Stack>`)).toEqual(["button-stretched"]);
    expect(
      rules(`<Stack>{rows.map((r) => <Tooltip label="x"><Button>{r}</Button></Tooltip>)}</Stack>`),
    ).toEqual(["button-stretched"]);
    expect(rules(`<div className="form-card"><Button>Run</Button></div>`)).toEqual([
      "outside-shared-layout",
      "button-stretched",
    ]);
    expect(rules(`<Group><Button style={{ gridColumn: "1 / -1" }}>Run</Button></Group>`)).toEqual([
      "button-stretched",
    ]);
  });

  it("a button that keeps its own width, or is a named exception, is not stretched", () => {
    expect(rules(`<Stack align="flex-start"><Button>Download</Button></Stack>`)).toEqual([]);
    expect(rules(`<Stack><Button w="fit-content">Download</Button></Stack>`)).toEqual([]);
    expect(
      rules(`<Stack><Button data-testid="org-row">Acme</Button></Stack>`, {
        stretchedButtonExceptions: ["org-row"],
      }),
    ).toEqual([]);
  });
});

describe("the form rules hold where an explanation sits and where ticks sit", () => {
  const long = "x".repeat(91);

  it("text between a label and its control", () => {
    expect(
      rules(
        `<FormLayout><FormField size="medium"><TextInput label="Host" description="Where it is." /></FormField></FormLayout>`,
      ),
    ).toEqual(["explanation-misplaced"]);
  });

  it("a brief hint below the control and a long one behind a (?) pass", () => {
    expect(
      rules(
        `<FormLayout><FormField size="medium" hint="Where it is." help="${long}"><TextInput label="Host" /></FormField></FormLayout>`,
      ),
    ).toEqual([]);
  });

  it("a hint too long to sit under a field, literal or translated", () => {
    expect(
      rules(
        `<FormLayout><FormField size="medium" hint="${long}"><TextInput label="Host" /></FormField></FormLayout>`,
      ),
    ).toEqual(["explanation-misplaced"]);
    expect(
      rules(
        `<FormLayout><FormField size="medium" hint={t("a.b")}><TextInput label="Host" /></FormField></FormLayout>`,
        { textOf: (key: string) => (key === "a.b" ? long : undefined) },
      ),
    ).toEqual(["explanation-misplaced"]);
  });

  it("a tick outside the form's group of ticks, or with a paragraph beside it", () => {
    expect(
      rules(
        `<FormLayout><FormField size="short"><Checkbox label="TLS" /></FormField></FormLayout>`,
      ),
    ).toEqual(["tick-ungrouped"]);
    expect(
      rules(
        `<FormLayout><FormChecks><FormField size="short" hint="Encrypts."><Switch label="TLS" /></FormField></FormChecks></FormLayout>`,
      ),
    ).toEqual(["explanation-misplaced"]);
  });

  it("ticks in the group, with help behind a (?), and the options of one choice, pass", () => {
    // a tick that is a component of its own: the form that mounts it groups it
    expect(
      rules(
        `const Draft = () => (<FormField size="short" help="Why."><Checkbox label="Draft" /></FormField>);`,
      ),
    ).toEqual([]);
    expect(
      rules(
        `<FormLayout><FormChecks><Checkbox label="TLS" /><FormField size="short" help="Why."><Switch label="Cache" /></FormField></FormChecks></FormLayout>`,
      ),
    ).toEqual([]);
    expect(
      rules(
        `<FormLayout><FormField size="full"><Checkbox.Group label="Subjects"><Checkbox value="a" label="A" /></Checkbox.Group></FormField></FormLayout>`,
      ),
    ).toEqual([]);
  });

  it("a FormField laid out by anything but the shared layout", () => {
    expect(
      rules(
        `<FormLayout><Stack><FormField size="medium"><TextInput label="Host" /></FormField></Stack></FormLayout>`,
      ),
    ).toEqual(["outside-shared-layout"]);
    expect(
      rules(
        `<FormLayout><FormField size="full"><Group><FormField size="short"><NumberInput label="N" /></FormField></Group></FormField></FormLayout>`,
      ),
    ).toEqual(["outside-shared-layout"]);
    expect(rules(`<><FormField size="medium"><TextInput label="Host" /></FormField></>`)).toEqual(
      [],
    );
    // A fold lays its fields out with the shared layout.
    expect(
      rules(
        `<FormLayout><FormField size="full"><FormFolds><FormFold value="a" title="A"><FormField size="short"><NumberInput label="N" /></FormField></FormFold></FormFolds></FormField></FormLayout>`,
      ),
    ).toEqual([]);
  });

  it("a native select in a form that does not take the shared look", () => {
    expect(
      rules(
        `<FormLayout><FormField size="medium"><label>Type<select /></label></FormField></FormLayout>`,
      ),
    ).toEqual(["outside-shared-layout"]);
    expect(
      rules(
        `<FormLayout><FormField size="medium"><label>Type<select className="form-select" /></label></FormField></FormLayout>`,
      ),
    ).toEqual([]);
    expect(
      rules(
        `<FormLayout><FormField size="medium"><NativeSelect label="Scope" /></FormField></FormLayout>`,
      ),
    ).toEqual(["outside-shared-layout"]);
  });

  it("a reusable select counts as a field", () => {
    expect(rules(`<Stack><RegionSelect value={v} /></Stack>`)).toEqual(["outside-shared-layout"]);
  });
});

describe("the form rules hold the one Save and the one Cancel", () => {
  const words = { textOf: (key: string) => ({ "x.save": "Save", "x.cancel": "Cancel" })[key] };

  it("a Save or Cancel button of a form's own", () => {
    expect(rules(`<FormFooter><Button>{t("x.save")}</Button></FormFooter>`, words)).toEqual([
      "save-cancel-not-shared",
    ]);
    expect(rules(`<Group><Button variant="default">Cancel</Button></Group>`)).toEqual([
      "save-cancel-not-shared",
    ]);
  });

  it("the shared footer's pair, and a button that is neither, pass", () => {
    expect(rules(`<FormFooter onSave={save} onCancel={close} />`, words)).toEqual([]);
    expect(
      rules(`<FormFooter onSave={save}><Button>Fill from profile</Button></FormFooter>`),
    ).toEqual([]);
  });
});

describe("the shared layout's CSS", () => {
  const css = readFileSync(join(SRC, "App.css"), "utf8").replace(/\/\*[\s\S]*?\*\//g, "");
  const rule = (selector: string): string => {
    const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const m = new RegExp(`(?:^|[},])\\s*${escaped}\\s*\\{([^}]*)\\}`).exec(css);
    if (!m) throw new Error(`no ${selector} rule in App.css`);
    return m[1];
  };

  it("puts the action row at the logical start, so right-to-left needs no rule of its own", () => {
    expect(rule(".form-footer")).toMatch(/justify-content:\s*flex-start/);
  });

  it("gives every size class a minimum and a maximum", () => {
    for (const size of ["short", "medium", "long", "full"]) {
      expect(rule(`.form-field--${size}`)).toMatch(/--form-field-min:\s*\S+;/);
      expect(rule(`.form-field--${size}`)).toMatch(/--form-field-max:\s*\S+;/);
    }
  });

  it("holds a field's label in its minimum width in any locale: the cell is never narrower than its content", () => {
    expect(rule(".form-field")).toMatch(/min-width:\s*min-content/);
    expect(css).toMatch(/\.mantine-InputWrapper-label[^{]*\{\s*white-space:\s*nowrap/);
  });

  it("never reorders: reading order and keyboard order are the DOM order", () => {
    const layoutRules = css.match(/\.form-(layout|field|footer|identifiers)[^{]*\{[^}]*\}/g) ?? [];
    expect(layoutRules.length).toBeGreaterThan(5);
    for (const r of layoutRules) expect(r).not.toMatch(/\border:|-reverse/);
  });
});

describe("a surface that edits in place", () => {
  const compact = `<Group><Button size="compact-xs">Apply</Button><Button size="compact-xs" variant="default">Cancel</Button></Group>`;
  it("may keep its compact Apply and Cancel, and its controls outside a form", () => {
    expect(rules(compact, { inPlaceEdit: true })).toEqual([]);
    expect(rules(`<TextInput size="xs" label="Reason" />`, { inPlaceEdit: true })).toEqual([]);
  });
  it("is held to the shared footer for a full-size Cancel, and anywhere else for a compact one", () => {
    expect(rules(`<Button variant="default">Cancel</Button>`, { inPlaceEdit: true })).toContain(
      "save-cancel-not-shared",
    );
    expect(rules(compact)).toContain("save-cancel-not-shared");
  });
});

describe("every component follows the form rules", () => {
  const files = componentFiles(SRC);
  const pending = new Set(NOT_YET_CONVERTED);

  it("finds the components", () => {
    expect(files.length).toBeGreaterThan(200);
  });

  it("lists only files that exist, each once", () => {
    for (const file of [
      ...NOT_YET_CONVERTED,
      ...Object.keys(NOT_A_FORM),
      ...Object.keys(EXPLORERS),
      ...Object.keys(IN_PLACE_EDIT),
      ...Object.keys(STRETCHED_BUTTONS),
    ]) {
      expect(existsSync(join(SRC, file)), `${file} is listed but does not exist`).toBe(true);
    }
    expect(pending.size).toBe(NOT_YET_CONVERTED.length);
  });

  it("still has the shared layout where the rules expect it", () => {
    expect(files).toContain(THE_SHARED_LAYOUT);
    // Were it to stop defining the footer, it would be an ordinary file and must be read again.
    expect(violationsOf(THE_SHARED_LAYOUT).map((v) => v.rule)).toContain("save-cancel-not-shared");
  });

  it.each(files.filter((f) => !pending.has(f) && !(f in EXPLORERS) && f !== THE_SHARED_LAYOUT))(
    "%s",
    (file) => {
      const violations = violationsOf(file);
      expect(violations, describeViolations(file, violations)).toEqual([]);
    },
  );

  it.each([...NOT_YET_CONVERTED])(
    "%s is still to convert: once it passes it leaves the list",
    (file) => {
      expect(
        violationsOf(file).length,
        `${file} follows the form rules now: remove it from NOT_YET_CONVERTED`,
      ).toBeGreaterThan(0);
    },
  );

  it.each(Object.keys(IN_PLACE_EDIT))("%s still needs its in-place-edit entry", (file) => {
    expect(
      violationsOf(file, false, false).length,
      `${file} breaks no form rule without its entry: drop it`,
    ).toBeGreaterThan(0);
  });

  it.each(Object.keys(EXPLORERS))("%s still needs its explorer entry", (file) => {
    expect(
      violationsOf(file, false).length,
      `${file} breaks no form rule: drop its entry`,
    ).toBeGreaterThan(0);
  });

  it.each(Object.keys(NOT_A_FORM))("%s still needs its not-a-form entry", (file) => {
    const withoutEntry = violationsOf(file, false).filter(
      (v) => v.rule === "outside-shared-layout",
    );
    expect(
      withoutEntry.length,
      `${file} has no control outside a FormField: drop its entry`,
    ).toBeGreaterThan(0);
  });

  it.each(Object.entries(STRETCHED_BUTTONS))(
    "%s still holds its stretched-button exceptions",
    (file, entries) => {
      const source = readFileSync(join(SRC, file), "utf8");
      for (const { marker, reason } of entries) {
        expect(source.includes(marker), `${file}: no button "${marker}" (${reason})`).toBe(true);
      }
    },
  );
});
