// Copyright (c) 2026 Kenneth Stott
// Canary: 0e7a3d58-c2b1-4f96-8d04-6a9f1e2b7c35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2280: OneDrive and Google Drive are File Crawler transports, each with its typed
// connection form: onedrive:// asks for the tenant, client id and client secret; gdrive:// for a
// service account key, or an OAuth client and its refresh token, saved in the source's mapping.

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { renderInForm, screen } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/AuthContext")>()),
  useAuth: () => ({ activeOrgId: null }),
}));
import {
  SourceFormFields,
  type SourceFormFieldsProps,
  type SourceFormState,
} from "../SourceFormFields";
import { driveFieldsFromMapping, driveMapping } from "../cloudDrive";
import { FILE_TRANSPORTS } from "../constants";
import { filesMappingJson } from "../excelHints";
import { parseFilesPath } from "../sourceHelpers";

const BASE: SourceFormState = {
  id: "drive",
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

function Harness({ transport, fields }: { transport: string; fields: Record<string, string> }) {
  const [form, setForm] = useState<SourceFormState>(BASE);
  const [filesTransport, setFilesTransport] = useState(transport);
  const [authFields, setAuthFields] = useState(fields);
  const props = {
    form,
    setForm,
    authType: "none",
    setAuthType: () => {},
    authFields,
    setAuthFields,
    editingSourceId: null,
    essential: true,
    domainsEnabled: false,
    domains: [],
    filesTransport,
    setFilesTransport,
  } as unknown as SourceFormFieldsProps;
  return <SourceFormFields {...props} />;
}

describe("the OneDrive and Google Drive transports", () => {
  it("are offered beside the other transports and read back from a saved path", () => {
    const values = FILE_TRANSPORTS.map((t) => t.value);
    expect(values).toContain("onedrive://");
    expect(values).toContain("gdrive://");
    expect(parseFilesPath("onedrive://a@b.com/Reports/*.csv")).toEqual({
      transport: "onedrive://",
      path: "a@b.com/Reports/*.csv",
    });
    expect(parseFilesPath("gdrive://1AbC/Reports/*.csv")).toEqual({
      transport: "gdrive://",
      path: "1AbC/Reports/*.csv",
    });
  });

  it("onedrive:// asks for the tenant, client id and client secret", () => {
    renderInForm(<Harness transport="onedrive://" fields={{}} />);
    expect(screen.getByTestId("files-onedrive-tenant-input")).toBeInTheDocument();
    expect(screen.getByTestId("files-onedrive-client-id-input")).toBeInTheDocument();
    expect(screen.getByTestId("files-onedrive-client-secret-input")).toHaveAttribute(
      "type",
      "password",
    );
    expect(screen.queryByTestId("files-gdrive-auth-select")).not.toBeInTheDocument();
  });

  it("gdrive:// asks for a service account key by default", () => {
    renderInForm(<Harness transport="gdrive://" fields={{}} />);
    expect(screen.getByTestId("files-gdrive-auth-select")).toHaveValue("Service account");
    expect(screen.getByTestId("files-gdrive-key-input")).toHaveAttribute("type", "password");
    expect(screen.queryByTestId("files-gdrive-refresh-token-input")).not.toBeInTheDocument();
  });

  it("gdrive:// with OAuth asks for the client and its refresh token", () => {
    renderInForm(<Harness transport="gdrive://" fields={{ gdrive_auth: "oauth" }} />);
    expect(screen.getByTestId("files-gdrive-client-id-input")).toBeInTheDocument();
    expect(screen.getByTestId("files-gdrive-client-secret-input")).toBeInTheDocument();
    expect(screen.getByTestId("files-gdrive-refresh-token-input")).toHaveAttribute(
      "type",
      "password",
    );
    expect(screen.queryByTestId("files-gdrive-key-input")).not.toBeInTheDocument();
  });

  it("saves the gdrive:// sign-in in the mapping, and none for another transport", () => {
    expect(driveMapping("gdrive://", {})).toEqual({ gdrive_auth: "service_account" });
    expect(
      driveMapping("gdrive://", { gdrive_auth: "oauth", refresh_token: "${secret:R}" }),
    ).toEqual({ gdrive_auth: "oauth", refresh_token: "${secret:R}" });
    expect(driveMapping("onedrive://", { gdrive_auth: "oauth" })).toEqual({});
    const previous = JSON.stringify({ gdrive_auth: "oauth", refresh_token: "x", crawl: {} });
    expect(JSON.parse(filesMappingJson(previous, {}, "s3://"))).toEqual({ crawl: {} });
    expect(driveFieldsFromMapping({ gdrive_auth: "oauth", refresh_token: "x", other: 1 })).toEqual({
      gdrive_auth: "oauth",
      refresh_token: "x",
    });
  });
});
