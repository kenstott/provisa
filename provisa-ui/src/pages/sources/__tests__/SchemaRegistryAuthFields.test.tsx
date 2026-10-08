// Copyright (c) 2026 Kenneth Stott
// Canary: b7e651c1-eedf-4d3e-8640-0059aa62ed40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1951: each schema registry authentication method shows exactly its own fields, the CA
// bundle path always, and each field writes the CdcState key it is saved under.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { SchemaRegistryAuthFields } from "../SchemaRegistryAuthFields";
import { schemaRegistryAuthPayload } from "../schemaRegistryAuth";
import type { CdcState } from "../SourceFormFields";

const BASE: CdcState = {
  bootstrapServers: "k:9092",
  topicPrefix: "p",
  schemaRegistryUrl: "http://r:8081",
  schemaRegistryAuth: "none",
  schemaRegistryUsername: "",
  schemaRegistryPassword: "",
  schemaRegistryToken: "",
  schemaRegistryClientCert: "",
  schemaRegistryClientKey: "",
  schemaRegistryCa: "",
  consumerGroupId: "",
};

const FIELD_IDS = {
  username: "cdc-registry-username-input",
  password: "cdc-registry-password-input",
  token: "cdc-registry-token-input",
  cert: "cdc-registry-client-cert-input",
  key: "cdc-registry-client-key-input",
} as const;

const CASES: [string, (keyof typeof FIELD_IDS)[], string[]][] = [
  ["none", [], []],
  ["basic", ["username", "password"], ["schemaRegistryUsername", "schemaRegistryPassword"]],
  ["bearer", ["token"], ["schemaRegistryToken"]],
  ["mtls", ["cert", "key"], ["schemaRegistryClientCert", "schemaRegistryClientKey"]],
];

describe("SchemaRegistryAuthFields", () => {
  it.each(CASES)("%s shows exactly its fields plus the CA path", (method, shown, keys) => {
    const setCdc = vi.fn();
    render(
      <SchemaRegistryAuthFields cdc={{ ...BASE, schemaRegistryAuth: method }} setCdc={setCdc} />,
    );
    expect(screen.getByTestId("cdc-registry-auth-select")).toBeTruthy();
    for (const [name, id] of Object.entries(FIELD_IDS)) {
      const present = screen.queryByTestId(id) !== null;
      expect(present).toBe(shown.includes(name as keyof typeof FIELD_IDS));
    }
    shown.forEach((name, i) => {
      fireEvent.change(screen.getByTestId(FIELD_IDS[name]), { target: { value: "v" } });
      expect(setCdc).toHaveBeenLastCalledWith({
        ...BASE,
        schemaRegistryAuth: method,
        [keys[i]]: "v",
      });
    });
    fireEvent.change(screen.getByTestId("cdc-registry-ca-input"), { target: { value: "/ca.pem" } });
    expect(setCdc).toHaveBeenLastCalledWith({
      ...BASE,
      schemaRegistryAuth: method,
      schemaRegistryCa: "/ca.pem",
    });
  });

  it("choosing a method writes schemaRegistryAuth", async () => {
    const setCdc = vi.fn();
    render(<SchemaRegistryAuthFields cdc={BASE} setCdc={setCdc} />);
    fireEvent.click(screen.getByTestId("cdc-registry-auth-select"));
    fireEvent.click(await screen.findByRole("option", { name: "Bearer token" }));
    expect(setCdc).toHaveBeenLastCalledWith({ ...BASE, schemaRegistryAuth: "bearer" });
  });
});

describe("schemaRegistryAuthPayload", () => {
  const filled: CdcState = {
    ...BASE,
    schemaRegistryUsername: "u",
    schemaRegistryPassword: "p",
    schemaRegistryToken: "t",
    schemaRegistryClientCert: "/c",
    schemaRegistryClientKey: "/k",
    schemaRegistryCa: "/ca",
  };

  it("sends only the chosen method's fields, and the CA with any method", () => {
    expect(schemaRegistryAuthPayload({ ...filled, schemaRegistryAuth: "basic" })).toEqual({
      schemaRegistryAuth: "basic",
      schemaRegistryUsername: "u",
      schemaRegistryPassword: "p",
      schemaRegistryToken: null,
      schemaRegistryClientCert: null,
      schemaRegistryClientKey: null,
      schemaRegistryCa: "/ca",
    });
    expect(schemaRegistryAuthPayload({ ...filled, schemaRegistryAuth: "mtls" })).toMatchObject({
      schemaRegistryUsername: null,
      schemaRegistryClientCert: "/c",
      schemaRegistryClientKey: "/k",
    });
  });

  it("sends none and nulls when there is no registry URL", () => {
    expect(schemaRegistryAuthPayload({ ...filled, schemaRegistryUrl: "" })).toEqual({
      schemaRegistryAuth: "none",
      schemaRegistryUsername: null,
      schemaRegistryPassword: null,
      schemaRegistryToken: null,
      schemaRegistryClientCert: null,
      schemaRegistryClientKey: null,
      schemaRegistryCa: null,
    });
  });
});
