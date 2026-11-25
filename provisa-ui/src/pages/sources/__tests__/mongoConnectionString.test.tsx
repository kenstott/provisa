// Copyright (c) 2026 Kenneth Stott
// Canary: 8f2cccdd-10a7-4945-84c3-c683a46b301c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1985: a MongoDB source's host can be a connection string; the string carries its hosts, so
// the form shows no port for it and saves none.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen } from "../../../test-utils/render";

// SourceFormFields renders SourceFormFieldsExtended, which calls useAuth() unconditionally;
// stub it so the form renders without an AuthProvider.
vi.mock("../../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/AuthContext")>()),
  useAuth: () => ({ activeOrgId: null }),
}));
import { isMongoConnectionString } from "../mongoConnection";
import {
  SourceFormFields,
  type SourceFormFieldsProps,
  type SourceFormState,
} from "../SourceFormFields";

const START: SourceFormState = {
  id: "atlas",
  type: "mongodb",
  host: "localhost",
  port: 27017,
  database: "sample_mflix",
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
  allowedDomains: "",
  description: "",
};

function Harness({
  start,
  onSave,
}: {
  start: SourceFormState;
  onSave: (f: SourceFormState) => void;
}) {
  const [form, setForm] = useState(start);
  const props = {
    form,
    setForm: (f: SourceFormState) => {
      onSave(f);
      setForm(f);
    },
    authType: "none",
    setAuthType: () => {},
    authFields: {},
    setAuthFields: () => {},
    editingSourceId: null,
    essential: true,
    domainsEnabled: false,
    domains: [],
  } as unknown as SourceFormFieldsProps;
  return <SourceFormFields {...props} />;
}

describe("a MongoDB source's host", () => {
  it("is a connection string when it starts with mongodb:// or mongodb+srv://", () => {
    expect(isMongoConnectionString("mongodb+srv://cluster0.example.mongodb.net/")).toBe(true);
    expect(isMongoConnectionString("mongodb://h1:27017,h2:27017/?replicaSet=rs0")).toBe(true);
    expect(isMongoConnectionString("mongo.internal")).toBe(false);
  });

  it("given as a connection string hides the port and saves none", () => {
    const onSave = vi.fn();
    renderInForm(<Harness start={START} onSave={onSave} />);
    expect(screen.getByLabelText(/^Port/)).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText(/^Host/), {
      target: { value: "mongodb+srv://cluster0.example.mongodb.net/?retryWrites=true" },
    });
    expect(screen.queryByLabelText(/^Port/)).not.toBeInTheDocument();
    expect(onSave.mock.lastCall?.[0].port).toBe(0);
    expect(
      screen.getByRole("img", { name: /mongodb\+srv:\/\/ connection string/ }),
    ).toBeInTheDocument();
  });

  it("given as a host name keeps the port", () => {
    const onSave = vi.fn();
    renderInForm(<Harness start={START} onSave={onSave} />);
    fireEvent.change(screen.getByLabelText(/^Host/), { target: { value: "mongo.internal" } });
    expect(screen.getByLabelText(/^Port/)).toBeInTheDocument();
    expect(onSave.mock.lastCall?.[0].port).toBe(27017);
  });

  it("of another source type is never read as a connection string", () => {
    const onSave = vi.fn();
    renderInForm(<Harness start={{ ...START, type: "postgresql", port: 5432 }} onSave={onSave} />);
    fireEvent.change(screen.getByLabelText(/^Host/), { target: { value: "mongodb://x" } });
    expect(screen.getByLabelText(/^Port/)).toBeInTheDocument();
    expect(onSave.mock.lastCall?.[0].port).toBe(5432);
  });
});
