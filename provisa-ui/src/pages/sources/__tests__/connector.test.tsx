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
import { ConnectorSetupFields } from "../ConnectorSetupFields";
import {
  connectorMappingJson,
  connectorPick,
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
};

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
