// Copyright (c) 2026 Kenneth Stott
// Canary: 0d6f3e82-a194-4c57-b2c0-e7f19a3d4c6e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor } from "../../../test-utils/render";

const auth = { activeOrgId: "acme" };
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => auth }));
vi.mock("../../../api/secrets", () => ({
  fetchSecrets: vi.fn(async () => ({ secrets: [{ name: "db_uri" }, { name: "other_dsn" }] })),
}));

import type { Connector } from "../../../api/connectors";
import { ConnectorSetupFields, ConnectorSourceValues } from "../ConnectorSetupFields";
import type { SourceFormState } from "../SourceFormFields";
import { codeFieldsFromMapping, codeMappingJson } from "../codeSource";
import {
  connectorChose,
  connectorFields,
  connectorFormValues,
  connectorMappingJson,
  connectorPick,
  fixedFormKeys,
  fixedFormValues,
  heldFields,
  missingConnectorFields,
  pickedConnector,
  requiredField,
} from "../connector";

const CRM: Connector = {
  name: "crm",
  git: "https://git.example/crm.git",
  ref: "main",
  commit: "a".repeat(40),
  language: "python",
  image: "registry.example/data/crm",
  digest: `sha256:${"b".repeat(64)}`,
  code_hash: "c".repeat(64),
  arch: "amd64",
  requires: [{ name: "db_uri", secret: true }, { name: "region" }],
  description: "",
  icon: "",
  settings: [],
};

const FILED: Connector = {
  ...CRM,
  description: "Our CRM",
  icon: "data:image/png;base64,AAAA",
  requires: [
    { name: "db_uri", secret: true },
    { name: "region", default_value: "eu-west-1" },
    { name: "script", multiline: true, fixed_value: "print(1)" },
  ],
  settings: [
    { name: "replicate", default_value: 0 },
    { name: "cache_ttl", fixed_value: 300 },
    { name: "cache_enabled", default_value: false },
  ],
};

const FORM = { replicate: 0, cacheTtl: "300", cacheEnabled: false } as SourceFormState;

describe("what a connector's standard file sets", () => {
  it("starts the form at its defaults and its fixed values", () => {
    expect(connectorFormValues(FILED)).toEqual({
      replicate: 0,
      cacheTtl: "300",
      cacheEnabled: false,
    });
    expect(connectorFields(FILED)).toEqual({
      [requiredField("region")]: "eu-west-1",
      [requiredField("script")]: "print(1)",
    });
  });

  it("names only the settings changed from their defaults", () => {
    expect(connectorChose(FORM, FILED)).toEqual([]);
    expect(connectorChose({ ...FORM, replicate: -1 }, FILED)).toEqual(["replicate"]);
  });

  it("reports the settings it fixes", () => {
    expect([...fixedFormKeys(FILED)]).toEqual(["cacheTtl"]);
    expect(fixedFormValues(FILED)).toEqual({ cacheTtl: "300" });
    expect([...fixedFormKeys(null)]).toEqual([]);
  });

  it("sends a fixed value as the connector says, whatever the form holds", () => {
    const sent = JSON.parse(
      connectorMappingJson({ [requiredField("script")]: "changed" }, FILED, ["replicate"]),
    );
    expect(sent.settings.script).toBe("print(1)");
    expect(sent.connector_join.chose).toEqual(["replicate"]);
  });

  it("holds a saved source's fixed values to what the connector says now", () => {
    const held = heldFields({ code_connector_settings: '{"script":"old","region":"ap"}' }, FILED);
    expect(JSON.parse(held.code_connector_settings)).toEqual({ script: "print(1)", region: "ap" });
  });

  it("shows a fixed value read-only, and multi-line text as a text area", () => {
    renderInForm(
      <ConnectorSetupFields
        connector={FILED}
        fields={connectorFields(FILED)}
        setFields={() => undefined}
      />,
    );
    const script = screen.getByTestId("connector-require-script") as HTMLTextAreaElement;
    expect(script.tagName).toBe("TEXTAREA");
    expect(script.readOnly).toBe(true);
    expect(script.value).toBe("print(1)");
    const region = screen.getByTestId("connector-require-region") as HTMLInputElement;
    expect(region.readOnly).toBe(false);
    expect(region.value).toBe("eu-west-1");
  });

  it("keeps a saved source's values whole and its connector named", () => {
    const fields = codeFieldsFromMapping(
      JSON.stringify({
        runtime: "python",
        code: { kind: "git", repository: "r", commit: "c" },
        mode: "built",
        remote: {},
        content_hash: "h",
        connector: "crm",
        settings: { script: "line one\nline two" },
      }),
    );
    expect(fields.code_settings).toBe("");
    const saved = JSON.parse(codeMappingJson("python", fields));
    expect(saved.connector).toBe("crm");
    expect(saved.settings).toEqual({ script: "line one\nline two" });
  });

  it("edits a saved source's values, a fixed one read-only", () => {
    function Saved() {
      const [fields, setFields] = useState<Record<string, string>>({
        code_connector_settings: '{"script":"print(1)","region":"ap"}',
      });
      return (
        <>
          <ConnectorSourceValues connector={FILED} fields={fields} setFields={setFields} />
          <output data-testid="saved">{fields.code_connector_settings}</output>
        </>
      );
    }
    renderInForm(<Saved />);
    expect((screen.getByTestId("connector-require-script") as HTMLTextAreaElement).readOnly).toBe(
      true,
    );
    fireEvent.change(screen.getByTestId("connector-require-region"), {
      target: { value: "us" },
    });
    expect(JSON.parse(screen.getByTestId("saved").textContent ?? "")).toEqual({
      script: "print(1)",
      region: "us",
    });
  });
});

function Form({ initial = {} }: { initial?: Record<string, string> }) {
  const [fields, setFields] = useState<Record<string, string>>(initial);
  return (
    <>
      <ConnectorSetupFields connector={CRM} fields={fields} setFields={setFields} />
      <output data-testid="fields">{JSON.stringify(fields)}</output>
    </>
  );
}

describe("picking a connector", () => {
  it("tells a connector from a built-in type", () => {
    expect(pickedConnector(connectorPick("crm"), [CRM])).toBe(CRM);
    expect(pickedConnector("python", [CRM])).toBeNull();
    expect(pickedConnector(connectorPick("none"), [CRM])).toBeNull();
  });
});

describe("the mapping that joins the connector", () => {
  const fields = {
    connector_host: "connector.acme.internal",
    connector_port: "7443",
    connector_pairing_code: "fp.secret",
    [requiredField("db_uri")]: "db_uri",
    [requiredField("region")]: "eu-west-1",
  };

  it("carries the host, a secret by name and a setting by value", () => {
    expect(JSON.parse(connectorMappingJson(fields, CRM))).toEqual({
      connector_join: {
        connector: "crm",
        host: "connector.acme.internal",
        port: 7443,
        pairing_code: "fp.secret",
      },
      secrets: { db_uri: null },
      settings: { region: "eu-west-1" },
    });
  });

  it("names another organization secret by reference, never by value", () => {
    const mapping = JSON.parse(
      connectorMappingJson({ ...fields, [requiredField("db_uri")]: "other_dsn" }, CRM),
    );
    expect(mapping.secrets).toEqual({ db_uri: "${secret:other_dsn}" });
  });

  it("lists what is missing", () => {
    expect(missingConnectorFields({}, CRM)).toEqual([
      "connector_host",
      "connector_port",
      "connector_pairing_code",
      requiredField("db_uri"),
      requiredField("region"),
    ]);
    expect(missingConnectorFields(fields, CRM)).toEqual([]);
  });
});

describe("ConnectorSetupFields", () => {
  it("offers a field for everything the connector requires", async () => {
    renderInForm(<Form />);
    await waitFor(() => expect(screen.getByTestId("connector-require-db_uri")).toBeTruthy());
    expect(screen.getByTestId("connector-host")).toBeTruthy();
    expect(screen.getByTestId("connector-port")).toBeTruthy();
    expect(screen.getByTestId("connector-pairing")).toBeTruthy();
    expect(screen.getByTestId("connector-require-region")).toBeTruthy();
  });

  it("shows the image it was built from, and nothing about how it was built", () => {
    renderInForm(<Form />);
    const image = screen.getByTestId("connector-image") as HTMLInputElement;
    expect(image.value).toBe(`${CRM.image}@${CRM.digest}`);
    expect(document.body.textContent).not.toMatch(/build command|path within/i);
  });

  it("keeps what is typed", () => {
    renderInForm(<Form />);
    fireEvent.change(screen.getByTestId("connector-host"), {
      target: { value: "h.example" },
    });
    expect(JSON.parse(screen.getByTestId("fields").textContent || "{}")).toEqual({
      connector_host: "h.example",
    });
  });
});
