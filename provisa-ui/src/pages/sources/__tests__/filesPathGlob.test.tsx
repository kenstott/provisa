// Copyright (c) 2026 Kenneth Stott
// Canary: 724313a7-881f-4a94-9d6d-250471884835
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1968: a File Crawler source's Path / Glob is a path, so it takes the long size (the Type
// field's width) beside Transport, and its long explanation sits behind a (?), not in the label.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { renderInForm, screen } from "../../../test-utils/render";

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

const BASE: SourceFormState = {
  id: "crawl",
  type: "files",
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
  allowedDomains: "",
  description: "",
};

function Harness() {
  const [form, setForm] = useState<SourceFormState>(BASE);
  const [filesTransport, setFilesTransport] = useState("file://");
  const props = {
    form,
    setForm,
    authType: "none",
    setAuthType: () => {},
    authFields: {},
    setAuthFields: () => {},
    editingSourceId: null,
    essential: true,
    domainsEnabled: false,
    domains: [],
    filesTransport,
    setFilesTransport,
  } as unknown as SourceFormFieldsProps;
  return <SourceFormFields {...props} />;
}

describe("a File Crawler source's Path / Glob", () => {
  it("is a long field beside the medium Transport, as Type is beside Id", () => {
    renderInForm(<Harness />);
    const path = screen.getByTestId("files-path-input");
    expect(path.closest(".form-field")).toHaveClass("form-field--long");
    const transport = screen.getByTestId("files-transport-select");
    expect(transport.closest(".form-field")).toHaveClass("form-field--medium");
  });

  it("is labelled Path / Glob, its explanation behind a (?)", () => {
    renderInForm(<Harness />);
    const path = screen.getByTestId("files-path-input");
    expect(path).toHaveAccessibleName(/^Path \/ Glob/);
    expect(path).toHaveAccessibleDescription(/Glob pattern relative to transport root/);
  });
});
