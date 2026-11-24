// Copyright (c) 2026 Kenneth Stott
// Canary: 3fc27929-a8a7-430d-b258-06a02f489dff
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984, REQ-2225: a connector's script value is edited in the app's code editor, with Python
// highlighting, and a form read shows it there too, read-only; other multi-line values stay a
// plain text area.

import { describe, expect, it, vi } from "vitest";
import { EditorView } from "@codemirror/view";
import { language } from "@codemirror/language";
import { FormRead } from "../../../components/form/FormLayout";
import { render, renderInForm, screen } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ activeOrgId: "acme" }) }));
vi.mock("../../../api/secrets", () => ({ fetchSecrets: vi.fn(async () => ({ secrets: [] })) }));
vi.mock("../../../api/sparkCluster", () => ({
  fetchSparkSource: vi.fn(async () => ({
    host: {},
    default_cluster: { origin: "local_test", label: "local test cluster", target: "127.0.0.1" },
  })),
}));

import type { Connector } from "../../../api/connectors";
import { ConnectorSetupFields, ConnectorSourceValues } from "../ConnectorSetupFields";
import { requiredField } from "../connector";

const SPARK: Connector = {
  name: "spark",
  git: "https://git.example/spark.git",
  ref: "main",
  commit: "a".repeat(40),
  language: "python",
  image: "registry.example/data/spark",
  digest: `sha256:${"b".repeat(64)}`,
  code_hash: "c".repeat(64),
  arch: "amd64",
  requires: [
    { name: "script", multiline: true, script: true },
    { name: "notes", multiline: true },
    { name: "cluster", org_default: "spark_cluster" },
  ],
  description: "",
  icon: "",
  settings: [],
};

const FIELDS = {
  connector_host: "spark-host.acme.internal",
  connector_port: "7443",
  connector_pairing_code: "fp.secret",
  [requiredField("script")]: "df = spark.range(3)\n",
};

function editorOf(testId: string): EditorView {
  const dom = screen.getByTestId(testId).querySelector(".cm-editor");
  expect(dom).not.toBeNull();
  const view = EditorView.findFromDOM(dom as HTMLElement);
  expect(view).not.toBeNull();
  return view as EditorView;
}

describe("a connector's script value", () => {
  it("renders in the code editor, with line numbers, holding its value", () => {
    renderInForm(
      <ConnectorSetupFields connector={SPARK} fields={FIELDS} setFields={() => undefined} />,
    );
    const field = screen.getByTestId("connector-require-script");
    expect(field.querySelector("textarea")).toBeNull();
    expect(field.querySelector(".cm-lineNumbers")).not.toBeNull();
    expect(editorOf("connector-require-script").state.doc.toString()).toBe("df = spark.range(3)\n");
    expect(screen.getByText("script")).toBeInTheDocument();
  });

  it("is highlighted as Python", () => {
    renderInForm(
      <ConnectorSetupFields connector={SPARK} fields={FIELDS} setFields={() => undefined} />,
    );
    expect(editorOf("connector-require-script").state.facet(language)?.name).toBe("python");
  });

  it("reads, in a form read, as the code in the editor, read-only", () => {
    const fields = {
      code_connector_settings: JSON.stringify({ script: "df = spark.range(3)\n" }),
    };
    render(
      <FormRead>
        <ConnectorSourceValues connector={SPARK} fields={fields} setFields={() => undefined} />
      </FormRead>,
    );
    const view = editorOf("connector-require-script");
    expect(view.state.doc.toString()).toBe("df = spark.range(3)\n");
    expect(view.state.readOnly).toBe(true);
    expect(view.state.facet(language)?.name).toBe("python");
    expect(screen.getByTestId("connector-require-script").closest("dd")).not.toBeNull();
  });

  it("sends each edit, whole, to the form's fields", () => {
    const setFields = vi.fn();
    renderInForm(<ConnectorSetupFields connector={SPARK} fields={FIELDS} setFields={setFields} />);
    const view = editorOf("connector-require-script");
    view.dispatch({ changes: { from: view.state.doc.length, insert: "df.show()\n" } });
    expect(setFields).toHaveBeenLastCalledWith({
      ...FIELDS,
      [requiredField("script")]: "df = spark.range(3)\ndf.show()\n",
    });
  });

  it("is read-only when the connector fixes it", () => {
    const fixed: Connector = {
      ...SPARK,
      requires: [{ name: "script", multiline: true, script: true, fixed_value: "print(1)" }],
    };
    renderInForm(
      <ConnectorSetupFields connector={fixed} fields={FIELDS} setFields={() => undefined} />,
    );
    const view = editorOf("connector-require-script");
    expect(view.state.doc.toString()).toBe("print(1)");
    expect(view.state.readOnly).toBe(true);
  });

  it("leaves a multi-line value that is not a script a text area", () => {
    renderInForm(
      <ConnectorSetupFields connector={SPARK} fields={FIELDS} setFields={() => undefined} />,
    );
    const notes = screen.getByTestId("connector-require-notes");
    expect(notes.tagName).toBe("TEXTAREA");
    expect(notes.closest(".cm-editor")).toBeNull();
  });
});
