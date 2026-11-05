// Copyright (c) 2026 Kenneth Stott
// Canary: 935594e3-4a77-4341-88b1-ef64961d6847
//
// This source code is licensed under the Business Source License 1.1

// REQ-1997: a file-based source's file -- its location, or the file itself by upload. A DuckDB
// file's location is kept in `database` (SourcesPage sends it as the path); while the location
// is the source's uploaded file it is not edited as text.
// REQ-2113: a CSV or Parquet source's file may instead be downloaded into the organization's
// folder from where it is (a file mount path, https:// or s3://; on a server of many
// organizations https:// or s3:// only); a new one may set up its table at once, as a draft.

import { Checkbox, Select, TextInput } from "@mantine/core";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FieldGroup, FormField } from "../../components/form/FormLayout";
import { useAuth } from "../../context/AuthContext";
import { useDomains } from "../../hooks/useAdminQueries";
import { isAssignableDomain } from "../../lib/internalDomains";
import { SourceFileUpload } from "./SourceFileUpload";
import type { SourceFormState } from "./SourceFormFields";
import {
  DOWNLOAD_TYPES,
  FILE_DATABASES,
  draftTable,
  fileLocationOf,
  isUploadPath,
} from "./sourceUpload";

interface Props {
  form: SourceFormState;
  setForm: (form: SourceFormState) => void;
  /** A source being created, not edited: it may set up its table with it. */
  creating: boolean;
}

export function FileLocationFields({ form, setForm, creating }: Props) {
  const { t } = useTranslation();
  const location = fileLocationOf(form);
  const setLocation = (value: string) =>
    setForm(form.type === "duckdb" ? { ...form, database: value } : { ...form, path: value });
  const uploaded = isUploadPath(location, form.id);
  const downloadable = DOWNLOAD_TYPES.has(form.type);
  const [mode, setMode] = useState<"location" | "download">(
    form.downloadFrom ? "download" : "location",
  );
  const choose = (next: string) => {
    setMode(next as "location" | "download");
    // Read in place: the source names no address to download from.
    if (next === "location") setForm({ ...form, downloadFrom: "" });
  };
  return (
    <FieldGroup>
      {downloadable && (
        <FormField size="medium">
          <Select
            label={t("sourceFormFields.fileMode")}
            value={mode}
            onChange={(next) => next && choose(next)}
            allowDeselect={false}
            data={[
              { value: "location", label: t("sourceFormFields.fileInPlace") },
              { value: "download", label: t("sourceFormFields.fileDownload") },
            ]}
            data-testid="source-file-mode"
          />
        </FormField>
      )}
      {downloadable && mode === "download" ? (
        <DownloadFields form={form} setForm={setForm} />
      ) : (
        <LocationFields
          form={form}
          location={location}
          uploaded={uploaded}
          setLocation={setLocation}
        />
      )}
      {downloadable && creating && <DraftTableFields form={form} setForm={setForm} />}
    </FieldGroup>
  );
}

function DownloadFields({ form, setForm }: Omit<Props, "creating">) {
  const { t } = useTranslation();
  const { multitenancy } = useAuth();
  return (
    <FormField
      size="long"
      help={t(
        multitenancy
          ? "sourceFormFields.downloadFromHintRemote"
          : "sourceFormFields.downloadFromHint",
      )}
    >
      <TextInput
        label={t("sourceFormFields.downloadFrom")}
        required
        value={form.downloadFrom ?? ""}
        onChange={(e) => setForm({ ...form, downloadFrom: e.currentTarget.value })}
        placeholder={`https://provisa.dev/demo-data/supplies.${form.type}`}
        data-testid="source-download-from"
      />
    </FormField>
  );
}

function DraftTableFields({ form, setForm }: Omit<Props, "creating">) {
  const { t } = useTranslation();
  const { domains } = useDomains();
  const { registerTableDraft: draft, tableDomainId: domain } = draftTable(form, domains);
  const choices = domains
    .filter((d) => isAssignableDomain(d.id))
    .map((d) => ({ value: d.id, label: d.id }));
  return (
    <>
      <FormField size="long" help={t("sourceFormFields.draftTableHint")}>
        <Checkbox
          label={t("sourceFormFields.draftTable")}
          checked={draft}
          onChange={(e) =>
            setForm({ ...form, registerTableDraft: e.currentTarget.checked, tableDomainId: domain })
          }
          data-testid="source-draft-table"
        />
      </FormField>
      {draft && (
        <FormField size="medium">
          <Select
            label={t("sourceFormFields.draftTableDomain")}
            required
            data={choices}
            value={domain}
            onChange={(value) => setForm({ ...form, tableDomainId: value })}
            data-testid="source-draft-table-domain"
          />
        </FormField>
      )}
    </>
  );
}

interface LocationProps {
  form: SourceFormState;
  location: string;
  uploaded: boolean;
  setLocation: (value: string) => void;
}

function LocationFields({ form, location, uploaded, setLocation }: LocationProps) {
  const { t } = useTranslation();
  return (
    <>
      <FormField
        size="long"
        hint={t(
          uploaded
            ? "sourceFormFields.uploadedFileHint"
            : FILE_DATABASES.has(form.type)
              ? "sourceFormFields.fileDatabaseLocationsHint"
              : "sourceFormFields.fileLocationsHint",
        )}
      >
        <TextInput
          label={
            form.type === "duckdb"
              ? t("sourceFormFields.filePath")
              : form.type === "sqlite"
                ? t("sourceFormFields.sqliteFilePath")
                : form.type === "csv"
                  ? t("sourceFormFields.csvFilePath")
                  : t("sourceFormFields.parquetFilePath")
          }
          required
          readOnly={uploaded}
          value={location}
          onChange={(e) => setLocation(e.currentTarget.value)}
          placeholder={
            form.type === "duckdb"
              ? t("sourceFormFields.filePathPlaceholder")
              : form.type === "sqlite"
                ? "./demo/files/orders.sqlite"
                : form.type === "csv"
                  ? "./demo/files/customers.csv"
                  : "./demo/files/products.parquet"
          }
          data-testid="source-file-location"
        />
      </FormField>
      <SourceFileUpload
        sourceId={form.id}
        type={form.type}
        uploaded={uploaded}
        onUploaded={setLocation}
      />
    </>
  );
}
