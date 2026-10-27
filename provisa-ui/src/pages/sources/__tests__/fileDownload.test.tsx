// Copyright (c) 2026 Kenneth Stott
// Canary: cfd60f1b-7b55-469f-998c-c2c92ddf0129
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2113: a CSV or Parquet source's file can instead be downloaded into the organization's
// folder; on a server of many organizations the form asks for an https:// or s3:// address only.
// A new single-file source offers to set up its table with it, as a draft, on by default, in the
// only or first domain until the steward picks another.

import { useEffect, useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, renderInForm, screen, waitFor } from "../../../test-utils/render";
import {
  SourceFormFields,
  type SourceFormFieldsProps,
  type SourceFormState,
} from "../SourceFormFields";
import { draftTable } from "../sourceUpload";
import { DomainsQuery as DOMAINS_QUERY } from "../../../hooks/admin.graphql";

const auth = vi.hoisted(() => ({ multitenancy: false }));
vi.mock("../../../context/AuthContext", async (original) => ({
  ...(await original<typeof import("../../../context/AuthContext")>()),
  useAuth: () => ({ multitenancy: auth.multitenancy }),
}));

const BASE: SourceFormState = {
  id: "supplies",
  type: "parquet",
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

const DOMAINS = [
  {
    request: { query: DOMAINS_QUERY },
    result: {
      data: {
        domains: [
          {
            __typename: "Domain",
            id: "shelter",
            description: "",
            graphqlAlias: null,
            steward: null,
            isSystem: false,
            parentDomainId: null,
            effectiveSteward: null,
          },
          {
            __typename: "Domain",
            id: "finance",
            description: "",
            graphqlAlias: null,
            steward: null,
            isSystem: false,
            parentDomainId: null,
            effectiveSteward: null,
          },
        ],
      },
    },
  },
];

let seen: SourceFormState | null = null;

/** Choose where the source's file is, in the form's "Its file" choice. */
function pick(label: string) {
  fireEvent.click(screen.getByTestId("source-file-mode"));
  fireEvent.click(screen.getByRole("option", { name: label }));
}

function Harness({
  start,
  editing = null,
}: {
  start: Partial<SourceFormState>;
  editing?: string | null;
}) {
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
    editingSourceId: editing,
    essential: true,
    domainsEnabled: false,
    domains: [],
  } as unknown as SourceFormFieldsProps;
  return <SourceFormFields {...props} />;
}

describe("a CSV or Parquet source's file, given by download", () => {
  it("is read in place until the steward chooses to download a copy", async () => {
    renderInForm(<Harness start={{}} />, { mocks: DOMAINS });
    expect(screen.getByTestId("source-file-location")).toBeInTheDocument();
    expect(screen.queryByTestId("source-download-from")).not.toBeInTheDocument();
    pick("Download a copy");
    const address = screen.getByTestId("source-download-from");
    fireEvent.change(address, {
      target: { value: "https://provisa.dev/demo-data/supplies.parquet" },
    });
    expect(seen?.downloadFrom).toBe("https://provisa.dev/demo-data/supplies.parquet");
    expect(screen.queryByTestId("source-file-location")).not.toBeInTheDocument();
    // Back to reading it in place: no address is kept.
    pick("Read it where it is");
    expect(seen?.downloadFrom).toBe("");
  });

  it("asks for an https:// or s3:// address on a server of many organizations", () => {
    auth.multitenancy = true;
    try {
      renderInForm(<Harness start={{ downloadFrom: "https://x/a.parquet" }} />, { mocks: DOMAINS });
      // The help, behind its (?), describes the address field.
      const address = screen.getByTestId("source-download-from");
      expect(address).toHaveAccessibleDescription(/by an https:\/\/ address or s3:\/\//);
      expect(address).not.toHaveAccessibleDescription(/its path on this server/);
    } finally {
      auth.multitenancy = false;
    }
  });

  it("is offered only to a CSV or Parquet source", () => {
    renderInForm(<Harness start={{ type: "sqlite", id: "db" }} />, { mocks: DOMAINS });
    expect(screen.queryByTestId("source-file-mode")).not.toBeInTheDocument();
    expect(screen.queryByTestId("source-draft-table")).not.toBeInTheDocument();
  });
});

describe("a new single-file source's table, set up with it as a draft", () => {
  it("is on by default, in the first domain until another is picked", async () => {
    renderInForm(<Harness start={{}} />, { mocks: DOMAINS });
    expect(screen.getByTestId("source-draft-table")).toBeChecked();
    expect(draftTable(BASE, [{ id: "shelter" }, { id: "finance" }])).toEqual({
      registerTableDraft: true,
      tableDomainId: "shelter",
    });
    await waitFor(() =>
      expect(screen.getByTestId("source-draft-table-domain")).toHaveValue("shelter"),
    );
    fireEvent.click(screen.getByTestId("source-draft-table"));
    expect(seen?.registerTableDraft).toBe(false);
    expect(screen.queryByTestId("source-draft-table-domain")).not.toBeInTheDocument();
  });

  it("is never put into a domain Provisa generates for itself", async () => {
    // meta and ops lead the domains list on a real server; no picker offers them as the domain
    // to put a table into (internalDomains.ts), and neither may this one's default.
    expect(draftTable(BASE, [{ id: "meta" }, { id: "ops" }, { id: "shelter" }])).toEqual({
      registerTableDraft: true,
      tableDomainId: "shelter",
    });
    const [shelter, finance] = DOMAINS[0].result.data.domains;
    const internal = [
      { ...shelter, id: "meta", isSystem: true },
      { ...shelter, id: "ops", isSystem: true },
    ];
    const mocks = [{ ...DOMAINS[0], result: { data: { domains: [...internal, shelter, finance] } } }];
    renderInForm(<Harness start={{}} />, { mocks });
    const picker = screen.getByTestId("source-draft-table-domain");
    await waitFor(() => expect(picker).toHaveValue("shelter"));
    fireEvent.click(picker);
    expect(await screen.findByRole("option", { name: "finance", hidden: true })).toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "meta", hidden: true })).not.toBeInTheDocument();
    expect(screen.queryByRole("option", { name: "ops", hidden: true })).not.toBeInTheDocument();
  });

  it("is not offered while a source is edited, nor sent for another type", () => {
    renderInForm(<Harness start={{}} editing="supplies" />, { mocks: DOMAINS });
    expect(screen.queryByTestId("source-draft-table")).not.toBeInTheDocument();
    expect(draftTable({ ...BASE, type: "sqlite" }, [{ id: "shelter" }])).toEqual({
      registerTableDraft: false,
      tableDomainId: null,
    });
  });
});
