// Copyright (c) 2026 Kenneth Stott
// Canary: 9b028897-de84-47bd-8bd2-aff7863c9cb0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A source's read view is its edit form drawn read-only: for every kind of source it lists the
// fields the form shows for that kind, under the same labels, each with the value it holds.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// A code source's fields (REQ-1984, REQ-1993) upload to the active organisation.
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ activeOrgId: "acme" }) }));
import { cleanup, render, screen, within } from "../../../test-utils/render";
import { FormLayout, FormRead } from "../../../components/form/FormLayout";
import { SourceFormFields, type SourceFormFieldsProps } from "../SourceFormFields";
import { SOURCE_TYPES } from "../constants";
import { seedSource } from "../sourceSeed";
import type { Source } from "../../../types/admin";

const noop = () => {};

function props(seedOf: Source): SourceFormFieldsProps {
  const seed = seedSource(seedOf, null);
  return {
    form: seed.form,
    setForm: noop,
    authType: seed.authType,
    setAuthType: noop,
    authFields: seed.authFields,
    setAuthFields: noop,
    editingSourceId: seedOf.id,
    cdc: seed.cdc,
    setCdc: noop,
    domainsEnabled: true,
    domains: [],
    spAuthType: seed.sharepoint.authType,
    setSpAuthType: noop,
    spCertPath: seed.sharepoint.certPath,
    setSpCertPath: noop,
    spCertPassword: seed.sharepoint.certPassword,
    setSpCertPassword: noop,
    spUsername: seed.sharepoint.username,
    setSpUsername: noop,
    spPassword: seed.sharepoint.password,
    setSpPassword: noop,
    splunkDisableSsl: seed.splunk.disableSsl,
    setSplunkDisableSsl: noop,
    splunkAuthMode: seed.splunk.authMode,
    setSplunkAuthMode: noop,
    filesTransport: seed.filesTransport,
    setFilesTransport: noop,
    filesAuthMode: "userpass",
    setFilesAuthMode: noop,
    filesCertPath: "",
    setFilesCertPath: noop,
    filesCertPassword: "",
    setFilesCertPassword: noop,
    govdataSubjects: seed.govdataSubjects ?? [],
    setGovdataSubjects: noop,
    govdataSubjectOptions: [],
    submitting: false,
    openapiSpecPath: "",
    setOpenapiSpecPath: noop,
    openapiSpecInline: "",
    setOpenapiSpecInline: noop,
    openapiSpecMode: "path",
    setOpenapiSpecMode: noop,
    openapiBaseUrl: "",
    setOpenapiBaseUrl: noop,
    openapiCacheTtl: "300",
    setOpenapiCacheTtl: noop,
    openapiPreview: null,
    openapiPreviewing: false,
    openapiPreviewError: null,
    onOpenapiPreview: noop,
    gqlNamespace: "",
    setGqlNamespace: noop,
    gqlCacheTtl: "300",
    setGqlCacheTtl: noop,
    grpcProtoPath: "",
    setGrpcProtoPath: noop,
    grpcServerAddress: "",
    setGrpcServerAddress: noop,
    grpcNamespace: "",
    setGrpcNamespace: noop,
    grpcTls: false,
    setGrpcTls: noop,
    grpcImportPaths: "",
    setGrpcImportPaths: noop,
    grpcCacheTtl: "300",
    setGrpcCacheTtl: noop,
    wsUseSsl: seed.websocket.useSsl,
    setWsUseSsl: noop,
    wsSubscribePayload: seed.websocket.subscribePayload,
    setWsSubscribePayload: noop,
    wsEventPath: seed.websocket.eventPath,
    setWsEventPath: noop,
    wsReconnectInterval: seed.websocket.reconnectInterval,
    setWsReconnectInterval: noop,
    rssFeedUrl: seed.rss.feedUrl,
    setRssFeedUrl: noop,
    rssPollInterval: seed.rss.pollInterval,
    setRssPollInterval: noop,
    rssUseSsl: seed.rss.useSsl,
    setRssUseSsl: noop,
  };
}

function source(type: string, extra: Partial<Source> = {}): Source {
  return {
    id: "src",
    type,
    host: "db.example.test",
    port: 5432,
    database: "sales",
    username: "reader",
    dialect: null,
    cacheEnabled: true,
    cacheTtl: null,
    replicate: null,
    region: null,
    loadProtected: false,
    offPeakWindow: null,
    offPeakTz: "UTC",
    sentinelPath: null,
    downloadFrom: null,
    freshnessGate: false,
    maxLiveConcurrency: null,
    gqlNamingConvention: null,
    path: null,
    allowedDomains: [],
    description: "",
    mappingJson: null,
    federationHintsJson: null,
    passwordRef: null,
    changeSignal: "",
    rowFetchByKeys: false,
    cdc: null,
    ...extra,
  };
}

const strip = (text: string) => text.replace(/\s*\*\s*$/, "").trim();

/** The labels of the edit form's fields: each innermost field's control's label or name. */
function editLabels(root: HTMLElement): string[] {
  return [...root.querySelectorAll<HTMLElement>(".form-field")]
    .filter((f) => f.querySelector(".form-field") === null)
    .map((f) => {
      // A group's own label (a choice of options) before its options' labels.
      const label = f.querySelector(".mantine-InputWrapper-label") ?? f.querySelector("label");
      if (label?.textContent) return strip(label.textContent);
      const named = f.querySelector("[aria-label]");
      return strip(named?.getAttribute("aria-label") ?? "");
    })
    .filter(Boolean)
    .sort();
}

/** The read rows: only those directly in the grid are drawn (the rest of the form is not). */
function readLabels(root: HTMLElement): string[] {
  return [...root.querySelectorAll(":scope > dt")].map((d) => strip(d.textContent ?? "")).sort();
}

beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response("[]", { status: 200 })),
  );
});
afterEach(() => vi.unstubAllGlobals());

describe("a source read", () => {
  it.each(SOURCE_TYPES.map((s) => s.value))("lists the fields the %s form shows", (type) => {
    const p = props(source(type));
    const edit = render(
      <FormLayout>
        <SourceFormFields {...p} />
      </FormLayout>,
    );
    const shownInEdit = editLabels(edit.container);
    cleanup();
    render(
      <FormRead testId="read">
        <SourceFormFields {...p} />
      </FormRead>,
    );
    const read = screen.getByTestId("read");
    expect(readLabels(read)).toEqual(shownInEdit);
    // Every row is in the grid itself: one wrapped in anything else would not be drawn.
    expect(read.querySelectorAll("dt")).toHaveLength(shownInEdit.length);
    // Nothing of the form is editable when read.
    expect(within(read).queryAllByRole("textbox")).toEqual([]);
    expect(within(read).queryAllByRole("checkbox")).toEqual([]);
  });

  it("shows each value as the form holds it: text, a choice by its label, a tick, a secret", () => {
    const p = props(
      source("postgresql", {
        description: "Orders",
        passwordRef: "${secret:pg}",
        cacheEnabled: false,
      }),
    );
    render(
      <FormRead testId="read">
        <SourceFormFields {...p} />
      </FormRead>,
    );
    const value = (label: string) =>
      [...screen.getByTestId("read").querySelectorAll("dt")].find(
        (d) => strip(d.textContent ?? "") === label,
      )?.nextElementSibling?.textContent;
    expect(value("Host")).toBe("db.example.test");
    expect(value("Description")).toBe("Orders");
    expect(value("Password")).toBe("Set");
    expect(value("Password")).not.toContain("secret");
  });

  it("shows an uploaded archive kept from before by its file's name and its hash", () => {
    const p = props(
      source("python", {
        mappingJson: JSON.stringify({
          runtime: "python",
          code: {
            kind: "upload",
            upload: "e80be40c584d433f9aa66108a9c9b9015264abfbe3ec43c992ae503ecc79d4ce",
            filename: "churn_model.tar.gz",
          },
        }),
      }),
    );
    render(
      <FormRead testId="read">
        <SourceFormFields {...p} />
      </FormRead>,
    );
    const archive = [...screen.getByTestId("read").querySelectorAll("dt")].find(
      (d) => strip(d.textContent ?? "") === "Archive",
    )?.nextElementSibling?.textContent;
    expect(archive).toBe("Kept: churn_model.tar.gz (e80be40c584d)");
  });
});
