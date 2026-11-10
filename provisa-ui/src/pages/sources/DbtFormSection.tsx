// Copyright (c) 2026 Kenneth Stott
// Canary: b92e7c45-1f08-4d63-a7c2-05d8f1e3b467
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2228: the setup form of a dbt source.

import { Button, FileInput, Group, PasswordInput, Radio, Text, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import { DbtCandidates } from "./DbtCandidates";
import { type DbtNewWarehouse, type DbtSourceForm, type DbtSpecMode } from "./dbt";

interface Props {
  dbt: DbtSourceForm;
  sourceId: string;
  /** The source is saved, so it holds a manifest whose candidates can be decided. */
  saved?: boolean;
}

export function DbtFormSection({ dbt, sourceId, saved = false }: Props) {
  const { t } = useTranslation();
  const fill = (database: string, change: Partial<DbtNewWarehouse>) =>
    dbt.setFills({ ...dbt.fills, [database]: { ...dbt.fills[database], ...change } });

  return (
    <>
      <FormField size="full">
        <Radio.Group
          name="dbtSpec"
          label={t("dbtFormSection.specLegend")}
          value={dbt.spec}
          onChange={(v) => dbt.setSpec(v as DbtSpecMode)}
        >
          <Group mt="xs" gap="lg">
            <Radio
              value="location"
              label={t("dbtFormSection.specLocation")}
              data-testid="dbt-spec-location"
            />
            <Radio
              value="upload"
              label={t("dbtFormSection.specUpload")}
              data-testid="dbt-spec-upload"
            />
            <Radio
              value="cloud"
              label={t("dbtFormSection.specCloud")}
              data-testid="dbt-spec-cloud"
            />
          </Group>
        </Radio.Group>
      </FormField>
      {dbt.spec === "location" && (
        <FormField size="long">
          <TextInput
            required
            label={t("dbtFormSection.locationLabel")}
            placeholder={t("dbtFormSection.locationPlaceholder")}
            value={dbt.location}
            onChange={(e) => dbt.setLocation(e.target.value)}
            data-testid="dbt-location-input"
          />
        </FormField>
      )}
      {dbt.spec === "upload" && (
        <FormField size="long">
          <FileInput
            required
            accept="application/json,.json"
            label={t("dbtFormSection.uploadLabel")}
            onChange={(file) => {
              if (file) void file.text().then(dbt.setContent);
            }}
            data-testid="dbt-upload-input"
          />
        </FormField>
      )}
      {dbt.spec === "cloud" && (
        <>
          <FormField size="long">
            <TextInput
              required
              label={t("dbtFormSection.cloudHostLabel")}
              value={dbt.host}
              onChange={(e) => dbt.setHost(e.target.value)}
              data-testid="dbt-cloud-host"
            />
          </FormField>
          <FormField size="medium">
            <TextInput
              required
              label={t("dbtFormSection.accountLabel")}
              value={dbt.accountId}
              onChange={(e) => dbt.setAccountId(e.target.value)}
              data-testid="dbt-cloud-account"
            />
          </FormField>
          <FormField size="medium">
            <TextInput
              required
              label={t("dbtFormSection.jobLabel")}
              value={dbt.jobId}
              onChange={(e) => dbt.setJobId(e.target.value)}
              data-testid="dbt-cloud-job"
            />
          </FormField>
          <FormField size="long" hint={t("dbtFormSection.tokenHint")}>
            <PasswordInput
              required
              label={t("dbtFormSection.tokenLabel")}
              value={dbt.token}
              onChange={(e) => dbt.setToken(e.target.value)}
              data-testid="dbt-cloud-token"
            />
          </FormField>
        </>
      )}
      {dbt.spec !== "cloud" && (
        <FormField size="long" hint={t("dbtFormSection.commandLineHint")}>
          <TextInput
            required
            label={t("dbtFormSection.commandLineLabel")}
            value={dbt.commandLine}
            onChange={(e) => dbt.setCommandLine(e.target.value)}
            data-testid="dbt-command-line"
          />
        </FormField>
      )}
      <FormField size="full">
        <Button
          type="button"
          variant="default"
          loading={dbt.previewing}
          onClick={() => void dbt.runPreview(sourceId)}
          data-testid="dbt-preview"
        >
          {t("dbtFormSection.previewButton")}
        </Button>
        {dbt.previewError && (
          <Text c="red" size="sm" mt="xs" data-testid="dbt-preview-error">
            {dbt.previewError}
          </Text>
        )}
        {dbt.preview && (
          <Text size="sm" mt="xs" data-testid="dbt-preview-summary">
            {t("dbtFormSection.summary", {
              project: dbt.preview.project,
              adapter: dbt.preview.adapter,
              models: dbt.preview.models,
              commands: dbt.preview.commands,
            })}
          </Text>
        )}
      </FormField>
      {dbt.preview?.warehouses.map((w) =>
        w.source_id ? (
          <FormField size="full" key={w.database}>
            <Text size="sm" data-testid="dbt-warehouse-found">
              {t("dbtFormSection.warehouseFound", { database: w.database, source: w.source_id })}
            </Text>
          </FormField>
        ) : (
          <FormField size="full" key={w.database}>
            {w.source_type ? (
              <>
                <Text size="sm" mb="xs" data-testid="dbt-warehouse-missing">
                  {t("dbtFormSection.warehouseMissing", {
                    adapter: dbt.preview?.adapter,
                    database: w.database,
                  })}
                </Text>
                <Group grow align="flex-start">
                  <TextInput
                    label={t("dbtFormSection.warehouseId")}
                    value={dbt.fills[w.database]?.sourceId ?? ""}
                    onChange={(e) => fill(w.database, { sourceId: e.target.value })}
                    data-testid="dbt-warehouse-id"
                  />
                  <TextInput
                    label={t("dbtFormSection.warehouseHost")}
                    value={dbt.fills[w.database]?.host ?? ""}
                    onChange={(e) => fill(w.database, { host: e.target.value })}
                    data-testid="dbt-warehouse-host"
                  />
                  <TextInput
                    label={t("dbtFormSection.warehouseUser")}
                    value={dbt.fills[w.database]?.username ?? ""}
                    onChange={(e) => fill(w.database, { username: e.target.value })}
                    data-testid="dbt-warehouse-user"
                  />
                  <PasswordInput
                    label={t("dbtFormSection.warehouseCredential")}
                    value={dbt.fills[w.database]?.password ?? ""}
                    onChange={(e) => fill(w.database, { password: e.target.value })}
                    data-testid="dbt-warehouse-credential"
                  />
                </Group>
              </>
            ) : (
              <Text size="sm" c="dimmed" data-testid="dbt-adapter-unknown">
                {t("dbtFormSection.adapterUnknown", { adapter: dbt.preview?.adapter })}
              </Text>
            )}
          </FormField>
        ),
      )}
      {saved && <DbtCandidates sourceId={sourceId} />}
    </>
  );
}
