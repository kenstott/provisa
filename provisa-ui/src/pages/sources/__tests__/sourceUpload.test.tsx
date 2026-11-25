// Copyright (c) 2026 Kenneth Stott
// Canary: dae01bc1-efe3-4a06-9209-41c9caa3cc91
//
// This source code is licensed under the Business Source License 1.1

// REQ-1997: a file-based source's file can be uploaded from its form. The file's path becomes
// the source's and is not edited as text while it is the upload; the form says which locations
// each file type takes, and asks a file database's store for its credentials too.

import { useEffect, useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor } from "../../../test-utils/render";

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
import { fileLocationOf, isUploadPath, uploadName } from "../sourceUpload";
import { CLOUD_FILE_TYPES } from "../objectStoreHints";

const BASE: SourceFormState = {
  id: "survey",
  type: "sqlite",
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

const UPLOADED = "{files}/survey/survey.sqlite";

let seen: SourceFormState | null = null;

function Harness({ start }: { start: Partial<SourceFormState> }) {
  const [form, setForm] = useState<SourceFormState>({ ...BASE, ...start });
  const [authFields, setAuthFields] = useState<Record<string, string>>({});
  useEffect(() => {
    seen = form;
  }, [form]);
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
  } as unknown as SourceFormFieldsProps;
  return <SourceFormFields {...props} />;
}

/** XMLHttpRequest that answers each PUT with `answer`, reporting half then all of the body. */
function stubUpload(status: number, answer: unknown) {
  const sent: { url: string; body: unknown }[] = [];
  class FakeRequest {
    status = 0;
    responseText = "";
    upload: { onprogress: ((e: unknown) => void) | null } = { onprogress: null };
    onload: (() => void) | null = null;
    onerror: (() => void) | null = null;
    private url = "";
    open(_method: string, url: string) {
      this.url = url;
    }
    setRequestHeader() {}
    send(body: unknown) {
      sent.push({ url: this.url, body });
      this.upload.onprogress?.({ lengthComputable: true, loaded: 1, total: 2 });
      this.status = status;
      this.responseText = JSON.stringify(answer);
      queueMicrotask(() => this.onload?.());
    }
  }
  vi.stubGlobal("XMLHttpRequest", FakeRequest);
  return sent;
}

afterEach(() => {
  vi.unstubAllGlobals();
  seen = null;
});

describe("upload helpers", () => {
  it("knows an upload's path by the source it belongs to", () => {
    expect(isUploadPath(UPLOADED, "survey")).toBe(true);
    // An upload saved before its path was kept as the org's token.
    expect(isUploadPath("/srv/provisa/orgs/acme/files/survey/survey.sqlite", "survey")).toBe(true);
    expect(isUploadPath("{files}/other/survey.sqlite", "survey")).toBe(false);
    expect(isUploadPath(UPLOADED, "other")).toBe(false);
    expect(isUploadPath("/data/survey.sqlite", "survey")).toBe(false);
    expect(isUploadPath("", "survey")).toBe(false);
  });

  it("keeps a file name the server takes", () => {
    expect(uploadName("survey results (1).sqlite")).toBe("survey_results__1_.sqlite");
    expect(uploadName("..hidden")).toBe("hidden");
  });

  it("reads a DuckDB file's location from database and the rest from path", () => {
    expect(fileLocationOf({ type: "duckdb", path: "", database: "/d.duckdb" })).toBe("/d.duckdb");
    expect(fileLocationOf({ type: "csv", path: "/c.csv", database: "" })).toBe("/c.csv");
  });

  it("asks the file databases' stores for their credentials too", () => {
    for (const type of ["csv", "parquet", "sqlite", "duckdb"]) {
      expect(CLOUD_FILE_TYPES.has(type)).toBe(true);
    }
  });
});

describe("the file source form", () => {
  it("uploads the file and makes its path the source's, read-only", async () => {
    const sent = stubUpload(200, { path: UPLOADED, bytes: 10 });
    renderInForm(<Harness start={{}} />);
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    const file = new File(["data"], "survey.sqlite");
    fireEvent.change(input, { target: { files: [file] } });
    await waitFor(() => expect(seen?.path).toBe(UPLOADED));
    expect(sent[0].url).toContain("/admin/sources/survey/file?name=survey.sqlite&type=sqlite");
    expect(sent[0].body).toBe(file);
    expect(screen.getByTestId("source-file-location")).toHaveAttribute("readonly");
    expect(screen.getByTestId("source-file-upload")).toHaveTextContent("Upload a new file");
  });

  it("puts a DuckDB file's upload where the form keeps its location", async () => {
    stubUpload(200, { path: UPLOADED.replace("survey.sqlite", "shop.duckdb"), bytes: 10 });
    renderInForm(<Harness start={{ type: "duckdb" }} />);
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [new File(["d"], "shop.duckdb")] } });
    await waitFor(() => expect(seen?.database).toContain("shop.duckdb"));
    expect(seen?.path).toBe("");
  });

  it("says why an upload was refused", async () => {
    stubUpload(413, {
      detail: "the file is larger than the 4 bytes an upload may be",
      code: "files.too_large",
      params: { limit: 4 },
    });
    renderInForm(<Harness start={{}} />);
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [new File(["12345"], "s.sqlite")] } });
    await waitFor(() => expect(screen.getByTestId("source-file-upload-error")).toBeInTheDocument());
    expect(seen?.path).toBe("");
  });

  it("asks for the source's ID before a file can be uploaded", () => {
    renderInForm(<Harness start={{ id: "" }} />);
    expect(screen.getByTestId("source-file-upload")).toBeDisabled();
    expect(screen.getByTestId("source-file-upload-needs-id")).toBeInTheDocument();
  });

  it("lets a path be typed when the file is not an upload, saying which locations it takes", () => {
    renderInForm(<Harness start={{ type: "csv" }} />);
    const field = screen.getByTestId("source-file-location");
    expect(field).not.toHaveAttribute("readonly");
    fireEvent.change(field, { target: { value: "s3://bucket/c.csv" } });
    expect(seen?.path).toBe("s3://bucket/c.csv");
    expect(
      screen.getByText(
        /an https:\/\/ address, or s3:\/\/, gs:\/\/ or az:\/\/ with the store's credentials; or upload/,
      ),
    ).toBeInTheDocument();
  });

  it("tells a file database that a location elsewhere is read-only", () => {
    renderInForm(<Harness start={{ type: "duckdb" }} />);
    expect(screen.getByText(/\(read-only there\)/)).toBeInTheDocument();
  });
});
