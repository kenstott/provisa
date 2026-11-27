// Copyright (c) 2026 Kenneth Stott
// Canary: a511320f-1fb8-4d36-946b-645ebe124b3c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A source's edit form drawn for a seeded source: the props SourceFormFields takes, each setter a
// no-op, and a source of a given type to seed it from.

import type { SourceFormFieldsProps } from "../pages/sources/SourceFormFields";
import { seedSource } from "../pages/sources/sourceSeed";
import type { Source } from "../types/admin";

const noop = () => {};

export function sourceFormProps(seedOf: Source): SourceFormFieldsProps {
  const seed = seedSource(seedOf, null);
  return {
    dbt: {} as SourceFormFieldsProps["dbt"],
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
    brandSetup: {},
    setBrandSetup: noop,
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

export function sourceOf(type: string, extra: Partial<Source> = {}): Source {
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
    isSystem: false,
    withdrawn: false,
    setupOutcomes: { tables: null, relationships: null, query: null, decidedAt: null },
    cdc: null,
    ...extra,
  };
}
