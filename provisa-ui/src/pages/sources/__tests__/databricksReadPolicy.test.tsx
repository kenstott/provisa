// Copyright (c) 2026 Kenneth Stott
// Canary: e9329fab-5196-4e21-8b72-6fbb160d04b6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1867: a Databricks source lands as a replica unless the steward picks the in-place read.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, within } from "../../../test-utils/render";

// SourceFormFields renders SourceFormFieldsExtended, which calls useAuth() unconditionally;
// stub it so the form renders without an AuthProvider.
vi.mock("../../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/AuthContext")>()),
  useAuth: () => ({ activeOrgId: null }),
}));
import {
  SourceFormFields,
  type SourceFormFieldsProps,
  type SourceFormState,
} from "../SourceFormFields";

const START: SourceFormState = {
  id: "dbx",
  type: "databricks",
  host: "acme.cloud.databricks.com",
  port: 443,
  database: "ext",
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
  onAuth,
}: {
  start: Record<string, string>;
  onAuth: (a: Record<string, string>) => void;
}) {
  const [authFields, setAuthFields] = useState(start);
  const props = {
    form: START,
    setForm: () => {},
    authType: "none",
    setAuthType: () => {},
    authFields,
    setAuthFields: (a: Record<string, string>) => {
      onAuth(a);
      setAuthFields(a);
    },
    editingSourceId: null,
    essential: true,
    domainsEnabled: false,
    domains: [],
  } as unknown as SourceFormFieldsProps;
  return <SourceFormFields {...props} />;
}

describe("a Databricks source's read policy", () => {
  it("defaults to the landed replica, with no in-place help", () => {
    renderInForm(<Harness start={{}} onAuth={vi.fn()} />);
    expect(screen.getByRole("textbox", { name: "How Provisa reads this source" })).toHaveValue(
      "From a landed replica (default)",
    );
    expect(screen.queryByText(/current snapshot through Unity Catalog/)).not.toBeInTheDocument();
  });

  it("set to in place saves reach scan and explains what it needs", () => {
    const onAuth = vi.fn();
    renderInForm(<Harness start={{}} onAuth={onAuth} />);
    const input = screen.getByRole("textbox", { name: "How Provisa reads this source" });
    fireEvent.click(input);
    // Mantine keeps the combobox dropdown out of the accessibility tree in jsdom: reach it through
    // the input's aria-controls listbox.
    const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
    if (listbox === null) throw new Error("the read-policy drop-down did not open");
    fireEvent.click(within(listbox).getByText(/In place, from Unity Catalog/));
    expect(onAuth.mock.lastCall?.[0].reach).toBe("scan");
    expect(screen.getByText(/current snapshot through Unity Catalog/)).toBeInTheDocument();
  });

  it("restored as scan from a saved source shows in place", () => {
    renderInForm(<Harness start={{ reach: "scan" }} onAuth={vi.fn()} />);
    expect(screen.getByRole("textbox", { name: "How Provisa reads this source" })).toHaveValue(
      "In place, from Unity Catalog (Iceberg and UniForm tables only)",
    );
  });
});
