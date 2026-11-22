// Copyright (c) 2026 Kenneth Stott
// Canary: fff3015e-a2db-4172-b346-38541d21de63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2247: the fields of one Syncing entry -- the same fields edit it and, drawn in a FormRead,
// read it. Every entry has its name, type, direction and domains. A data catalog also has its
// provider, how Provisa reaches it and signs in (REQ-2122), and, when its direction includes
// Export, its export settings: the role it is published as (REQ-2152), the DataZone project and
// the Delta Share option. A data catalog whose direction includes Import has its import schedule;
// the server's refusal of it is shown on the field (`importCronProblem`).
//
// Credentials are entered here and never read back: the server reports each as set/not-set, so an
// empty field means "leave the stored one alone" rather than "no secret".

import { useTranslation } from "react-i18next";
import {
  Checkbox,
  MultiSelect,
  NumberInput,
  PasswordInput,
  Select,
  TextInput,
} from "@mantine/core";
import {
  exportsTo,
  importsFrom,
  SYNC_DIRECTIONS,
  startingDirection,
  SYNC_ENTRY_TYPES,
  type SyncEntry,
  type SyncSecretField,
} from "../../../api/syncing";
import { assignableDomainIds } from "../../../lib/internalDomains";
import { FormChecks, FormField, FormIdentifiers } from "../../form/FormLayout";
import { useFormRead } from "../../form/formRead";

const ENTRA_MODE = "entra";
const BASIC_MODE = "basic";
const GOOGLE_MODE = "google_service_account";

/** A DataZone catalog signs AWS requests with its own keys: it has no endpoint or auth mode. */
const DATAZONE = "datazone";
/** Unity Catalog and Snowflake Horizon work through the engine itself: no endpoint either. */
const ENGINE_NATIVE = new Set(["unity_catalog", "snowflake_horizon"]);
/** A Unity Catalog catalog's export may also publish each product as a Delta Share. */
const UNITY_CATALOG = "unity_catalog";

export type Secrets = Partial<Record<SyncSecretField, string>>;

export interface EntryFieldsProps {
  entry: SyncEntry;
  secrets: Secrets;
  /** A new entry: its name and type are chosen; once kept, both are its own. */
  isNew: boolean;
  providers: string[];
  providerName: (provider: string) => string;
  publishableRoles: string[];
  domains: string[];
  /** Whether the organization's plan includes publishing: Export is offered only then. */
  entitled: boolean;
  onChange: (patch: Partial<SyncEntry>) => void;
  onSecret: (field: SyncSecretField, value: string) => void;
  /** The server's refusal of the import schedule, shown on its field. */
  importCronProblem?: string;
}

export function EntryFields({
  entry,
  secrets,
  isNew,
  providers,
  providerName,
  publishableRoles,
  domains,
  entitled,
  onChange,
  onSecret,
  importCronProblem,
}: EntryFieldsProps) {
  const { t } = useTranslation();
  const read = useFormRead();
  const isCatalog = entry.type === "data_catalog";
  const authModes = [
    { value: "api_key", label: t("syncingTab.authApiKey") },
    { value: "bearer", label: t("syncingTab.authBearer") },
    { value: BASIC_MODE, label: t("syncingTab.authBasic") },
    { value: ENTRA_MODE, label: t("syncingTab.authEntra") },
    { value: GOOGLE_MODE, label: t("syncingTab.authGoogle") },
  ];
  // Below the plan's tier a data catalog imports only; an entry kept exporting still reads so.
  const directions = SYNC_DIRECTIONS.filter(
    (d) => !isCatalog || entitled || !exportsTo(d) || d === entry.direction,
  );

  const secretField = (field: SyncSecretField, label: string, isSet: boolean) => (
    <FormField help={t("syncingTab.secretHelp")} size="medium">
      <PasswordInput
        label={label}
        data-testid={`syncing-${field.replace(/_/g, "-")}`}
        placeholder={isSet ? t("syncingTab.secretSet") : t("syncingTab.secretUnset")}
        value={read ? (isSet ? "set" : "") : (secrets[field] ?? "")}
        onChange={(e) => onSecret(field, e.currentTarget.value)}
      />
    </FormField>
  );

  const connection = () => {
    if (ENGINE_NATIVE.has(entry.provider)) return null;
    if (entry.provider === DATAZONE)
      return (
        <>
          <FormField help={t("syncingTab.awsRegionHelp")} size="short">
            <TextInput
              label={t("syncingTab.awsRegion")}
              data-testid="syncing-aws-region"
              value={entry.aws_region}
              onChange={(e) => onChange({ aws_region: e.currentTarget.value })}
            />
          </FormField>
          <FormField help={t("syncingTab.datazoneDomainIdHelp")} size="medium">
            <TextInput
              label={t("syncingTab.datazoneDomainId")}
              data-testid="syncing-datazone-domain-id"
              value={entry.datazone_domain_id}
              onChange={(e) => onChange({ datazone_domain_id: e.currentTarget.value })}
            />
          </FormField>
          {secretField(
            "aws_access_key_id",
            t("syncingTab.awsAccessKeyId"),
            entry.aws_access_key_id_set,
          )}
          {secretField(
            "aws_secret_access_key",
            t("syncingTab.awsSecretAccessKey"),
            entry.aws_secret_access_key_set,
          )}
        </>
      );
    return (
      <>
        <FormField size="long">
          <TextInput
            label={t("syncingTab.endpoint")}
            data-testid="syncing-endpoint"
            value={entry.endpoint}
            onChange={(e) => onChange({ endpoint: e.currentTarget.value })}
          />
        </FormField>
        <FormField size="medium">
          <Select
            label={t("syncingTab.authMode")}
            data-testid="syncing-auth-mode"
            data={authModes}
            value={entry.auth_mode}
            onChange={(v) => onChange({ auth_mode: v ?? "api_key" })}
          />
        </FormField>
        {entry.provider === "dataplex" && (
          <>
            <FormField help={t("syncingTab.gcpProjectHelp")} size="medium">
              <TextInput
                label={t("syncingTab.gcpProject")}
                data-testid="syncing-gcp-project"
                value={entry.gcp_project}
                onChange={(e) => onChange({ gcp_project: e.currentTarget.value })}
              />
            </FormField>
            <FormField help={t("syncingTab.gcpLocationHelp")} size="short">
              <TextInput
                label={t("syncingTab.gcpLocation")}
                data-testid="syncing-gcp-location"
                value={entry.gcp_location}
                onChange={(e) => onChange({ gcp_location: e.currentTarget.value })}
              />
            </FormField>
          </>
        )}
        {entry.auth_mode === GOOGLE_MODE ? (
          secretField(
            "google_service_account_json",
            t("syncingTab.googleServiceAccountJson"),
            entry.google_service_account_json_set,
          )
        ) : entry.auth_mode === ENTRA_MODE ? (
          <>
            <FormField size="medium">
              <TextInput
                label={t("syncingTab.entraTenantId")}
                data-testid="syncing-entra-tenant-id"
                value={entry.entra_tenant_id}
                onChange={(e) => onChange({ entra_tenant_id: e.currentTarget.value })}
              />
            </FormField>
            <FormField size="medium">
              <TextInput
                label={t("syncingTab.entraClientId")}
                data-testid="syncing-entra-client-id"
                value={entry.entra_client_id}
                onChange={(e) => onChange({ entra_client_id: e.currentTarget.value })}
              />
            </FormField>
            {secretField(
              "entra_client_secret",
              t("syncingTab.entraClientSecret"),
              entry.entra_client_secret_set,
            )}
          </>
        ) : entry.auth_mode === BASIC_MODE ? (
          <>
            <FormField size="medium">
              <TextInput
                label={t("syncingTab.username")}
                data-testid="syncing-username"
                value={entry.username}
                onChange={(e) => onChange({ username: e.currentTarget.value })}
              />
            </FormField>
            {/* The password rides in the same `token` field the bearer mode uses: one stored
            secret per entry, read by whichever mode is selected. */}
            {secretField("token", t("syncingTab.password"), entry.token_set)}
          </>
        ) : entry.auth_mode === "bearer" ? (
          secretField("token", t("syncingTab.token"), entry.token_set)
        ) : (
          secretField("api_key", t("syncingTab.apiKey"), entry.api_key_set)
        )}
      </>
    );
  };

  const exportSettings = () =>
    isCatalog && exportsTo(entry.direction) ? (
      <>
        {/* REQ-2152: a catalog is sent exactly what the role it is published as reads. */}
        <FormField help={t("syncingTab.roleHelp")} size="medium">
          <Select
            label={t("syncingTab.role")}
            data-testid="syncing-role"
            // REQ-2152: only roles whose reads the caller holds; a role saved by someone else
            // stays shown so the field says what the entry is published as.
            data={[...new Set([...publishableRoles, ...(entry.role ? [entry.role] : [])])]}
            value={entry.role}
            onChange={(v) => onChange({ role: v })}
            required
          />
        </FormField>
        {entry.provider === DATAZONE && (
          <FormField help={t("syncingTab.datazoneProjectIdHelp")} size="medium">
            <TextInput
              label={t("syncingTab.datazoneProjectId")}
              data-testid="syncing-datazone-project-id"
              value={entry.datazone_project_id}
              onChange={(e) => onChange({ datazone_project_id: e.currentTarget.value })}
            />
          </FormField>
        )}
        {entry.provider === UNITY_CATALOG && (
          <FormChecks>
            <FormField help={t("syncingTab.deltaShareHelp")} size="long">
              <Checkbox
                label={t("syncingTab.deltaShare")}
                data-testid="syncing-delta-share"
                checked={entry.delta_share}
                onChange={(e) => onChange({ delta_share: e.currentTarget.checked })}
              />
            </FormField>
          </FormChecks>
        )}
      </>
    ) : null;

  return (
    <>
      <FormIdentifiers>
        <FormField help={t("syncingTab.nameHelp")} size="medium">
          <TextInput
            label={t("syncingTab.name")}
            data-testid="syncing-name"
            value={entry.id}
            disabled={!isNew}
            required
            onChange={(e) => onChange({ id: e.currentTarget.value })}
          />
        </FormField>
      </FormIdentifiers>
      <FormField help={t("syncingTab.typeHelp")} size="medium">
        <Select
          label={t("syncingTab.type")}
          data-testid="syncing-type"
          data={SYNC_ENTRY_TYPES.map((v) => ({ value: v, label: t(`syncingTab.type_${v}`) }))}
          value={entry.type}
          disabled={!isNew}
          allowDeselect={false}
          onChange={(v) => {
            if (!v) return;
            const type = v as SyncEntry["type"];
            // A direction still at the old type's start takes the new type's start; one the
            // administrator chose is kept.
            const direction =
              entry.direction === startingDirection(entry.type)
                ? startingDirection(type)
                : entry.direction;
            // Only a data catalog has a provider and export settings: another type drops them.
            onChange(
              type === "data_catalog"
                ? { type, direction }
                : {
                    type,
                    direction,
                    provider: "",
                    role: null,
                    datazone_project_id: "",
                    delta_share: false,
                    import_cron: "",
                  },
            );
          }}
        />
      </FormField>
      <FormField help={t("syncingTab.directionHelp")} size="medium">
        <Select
          label={t("syncingTab.direction")}
          data-testid="syncing-direction"
          data={directions.map((v) => ({ value: v, label: t(`syncingTab.direction_${v}`) }))}
          value={entry.direction}
          allowDeselect={false}
          onChange={(v) => {
            if (!v) return;
            const direction = v as SyncEntry["direction"];
            // A direction that is off never runs, so an entry that stops importing drops its
            // import schedule.
            onChange(importsFrom(direction) ? { direction } : { direction, import_cron: "" });
          }}
        />
      </FormField>
      {isCatalog && (
        <FormField size="medium">
          <Select
            label={t("syncingTab.provider")}
            data-testid="syncing-provider"
            data={providers.map((p) => ({ value: p, label: providerName(p) }))}
            value={entry.provider || null}
            required
            onChange={(v) => onChange({ provider: v ?? "" })}
          />
        </FormField>
      )}
      <FormField help={t("syncingTab.domainsHelp")} size="long">
        <MultiSelect
          label={t("syncingTab.domains")}
          data-testid="syncing-domains"
          data={assignableDomainIds(domains).map((id) => ({ value: id, label: id }))}
          value={entry.domains}
          required={isCatalog}
          onChange={(v) => onChange({ domains: v })}
          searchable
        />
      </FormField>
      {isCatalog && entry.provider && (
        <>
          {connection()}
          <FormField size="short">
            <NumberInput
              label={t("syncingTab.timeout")}
              data-testid="syncing-timeout"
              value={entry.timeout_seconds}
              onChange={(v) => onChange({ timeout_seconds: Number(v) || 0 })}
            />
          </FormField>
        </>
      )}
      {isCatalog && importsFrom(entry.direction) && (
        <FormField help={t("syncingTab.importCronHelp")} size="medium">
          <TextInput
            label={t("syncingTab.importCron")}
            data-testid="syncing-import-cron"
            placeholder={t("syncingTab.importCronPlaceholder")}
            value={entry.import_cron}
            error={importCronProblem}
            onChange={(e) => onChange({ import_cron: e.currentTarget.value })}
          />
        </FormField>
      )}
      {exportSettings()}
    </>
  );
}
