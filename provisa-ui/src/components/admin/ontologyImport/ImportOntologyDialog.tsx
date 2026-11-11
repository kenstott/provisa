// Copyright (c) 2026 Kenneth Stott
// Canary: 9b2d6e41-3f7c-4a18-b5e0-c84f17a2d963
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2237: Import ontology. The steward picks an ontology file (an Apache Ossie model, an RDF/OWL
// file, or a whole release as a zip of its module files), its format where the file's name does
// not say it, and the domains its new terms are made in. The file is read at once and its review
// opens.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, FileInput, Modal, Select } from "@mantine/core";
import { FileUp } from "lucide-react";
import {
  ONTOLOGY_ACCEPT,
  ONTOLOGY_FORMATS,
  importOntology,
  type ImportReview,
  type OntologyFormat,
} from "../../../api/ontologyImport";
import { FormField, FormFooter, FormLayout } from "../../form/FormLayout";
import { MultiSelect } from "../../MultiSelect";

export function ImportOntologyDialog({
  domainOptions,
  domainsEnabled,
  defaultDomains,
  onClose,
  onImported,
}: {
  domainOptions: { id: string; label: string }[];
  domainsEnabled: boolean;
  defaultDomains: string[];
  onClose: () => void;
  onImported: (review: ImportReview) => void;
}) {
  const { t } = useTranslation();
  const [file, setFile] = useState<File | null>(null);
  const [format, setFormat] = useState<OntologyFormat | null>(null);
  const [domains, setDomains] = useState<string[]>(defaultDomains);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const ready = file !== null && (!domainsEnabled || domains.length > 0);
  const run = async () => {
    if (!file) return;
    setBusy(true);
    setError("");
    try {
      onImported(await importOntology(file, format, domainsEnabled ? domains : null));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      opened
      onClose={onClose}
      title={t("ontologyImport.importTitle")}
      size="lg"
      data-testid="ontology-import-dialog"
    >
      <FormLayout>
        <FormField size="long" help={t("ontologyImport.fileHelp")}>
          <FileInput
            label={t("ontologyImport.file")}
            accept={ONTOLOGY_ACCEPT}
            value={file}
            onChange={setFile}
            clearable
            required
            data-testid="ontology-import-file"
          />
        </FormField>
        <FormField size="short" help={t("ontologyImport.formatHelp")}>
          <Select
            label={t("ontologyImport.format")}
            placeholder={t("ontologyImport.formatFromName")}
            data={ONTOLOGY_FORMATS.map((f) => ({ value: f, label: t(`ontologyImport.format_${f}`) }))}
            value={format}
            onChange={(v) => setFormat(v as OntologyFormat | null)}
            clearable
            comboboxProps={{ withinPortal: true }}
            data-testid="ontology-import-format"
          />
        </FormField>
        {domainsEnabled && (
          <FormField size="long" help={t("ontologyImport.domainsHelp")}>
            <MultiSelect
              label={t("ontologyImport.domains")}
              options={domainOptions}
              value={domains}
              onChange={setDomains}
              required
            />
          </FormField>
        )}
        {error && (
          <FormField size="full">
            <Alert color="red" data-testid="ontology-import-error">
              {error}
            </Alert>
          </FormField>
        )}
        <FormFooter
          onCancel={onClose}
          action={
            <Button
              leftSection={<FileUp size={14} />}
              onClick={() => void run()}
              loading={busy}
              disabled={!ready}
              data-testid="ontology-import-run"
            >
              {t("ontologyImport.run")}
            </Button>
          }
        />
      </FormLayout>
    </Modal>
  );
}
