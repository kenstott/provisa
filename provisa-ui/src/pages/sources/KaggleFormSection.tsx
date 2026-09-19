// Copyright (c) 2026 Kenneth Stott
// Canary: 2c6f9a1d-4e8b-4c3a-9d7f-1a6e4c8b2d9f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Combobox, Group, Select, Text, TextInput, useCombobox } from "@mantine/core";
import type { Domain } from "../../types/admin";
import {
  useCreateSource,
  useKaggleDatasetsLazy,
  useKaggleTokenValidLazy,
  useRegisterTable,
  useRoles,
  useStageKaggleDataset,
} from "../../hooks/useAdminQueries";

// REQ-1783: Kaggle's own dataset search requires auth, so the form cannot present token entry
// and dataset search at once — the picker step only exists once step 1 validates live.
type Step = "token" | "picker";

interface KaggleFormSectionProps {
  domains: Domain[];
  onSourcesRegistered: (sourceIds: string[]) => void;
}

export function KaggleFormSection({ domains, onSourcesRegistered }: KaggleFormSectionProps) {
  const { t } = useTranslation();
  const [domainId, setDomainId] = useState("");
  const [token, setToken] = useState("");
  const [step, setStep] = useState<Step>("token");
  const [tokenError, setTokenError] = useState<string | null>(null);
  const [validating, setValidating] = useState(false);

  const [query, setQuery] = useState("");
  const [datasets, setDatasets] = useState<{ ref: string; title: string; subtitle: string }[]>([]);
  const [selectedRef, setSelectedRef] = useState<string | null>(null);
  const [registerError, setRegisterError] = useState<string | null>(null);
  const [registering, setRegistering] = useState(false);

  const validateToken = useKaggleTokenValidLazy();
  const searchDatasets = useKaggleDatasetsLazy();
  const { stageKaggleDataset } = useStageKaggleDataset();
  const { createSource } = useCreateSource();
  const { registerTable } = useRegisterTable();
  const { roles } = useRoles();
  const combobox = useCombobox();
  const searchDebounce = useRef<ReturnType<typeof setTimeout> | null>(null);

  // REQ-1783 amendment: re-entering this step (e.g. the token used before is now expired or
  // revoked) must show a clear "invalid, re-enter" state rather than a stale/empty picker — going
  // back to "token" and clearing prior results is exactly that, no separate flag needed since the
  // picker is unconditionally absent while step === "token".
  const handleValidate = async () => {
    setValidating(true);
    setTokenError(null);
    try {
      const valid = await validateToken(token);
      if (!valid) {
        setTokenError(t("kaggleFormSection.tokenInvalid"));
        setStep("token");
        setDatasets([]);
        return;
      }
      setStep("picker");
    } catch (e) {
      setTokenError(e instanceof Error ? e.message : String(e));
    } finally {
      setValidating(false);
    }
  };

  useEffect(() => {
    if (step !== "picker") return;
    if (searchDebounce.current) clearTimeout(searchDebounce.current);
    searchDebounce.current = setTimeout(async () => {
      const results = await searchDatasets(token, query);
      setDatasets(results);
      combobox.resetSelectedOption();
    }, 300);
    return () => {
      if (searchDebounce.current) clearTimeout(searchDebounce.current);
    };
    // combobox/searchDatasets/token deliberately excluded below: searchDatasets is a useCallback
    // from a useLazyQuery tuple (stable), combobox is the useCombobox() store (stable), and token
    // is read fresh via closure on every debounce fire — listing them would re-run this effect
    // (and cancel the in-flight debounce) on every keystroke into the token field, not just query.
    // eslint-disable-next-line react-hooks/exhaustive-deps -- see comment above
  }, [query, step]);

  const handleConfirmDataset = async () => {
    if (!selectedRef) return;
    const [owner, ref] = selectedRef.split("/");
    setRegistering(true);
    setRegisterError(null);
    try {
      const idPrefix = `kg_${Date.now()}_${ref.replace(/[^a-zA-Z0-9_-]/g, "_")}`;
      const staged = await stageKaggleDataset(token, owner, ref, idPrefix);
      if (!staged.success) {
        setRegisterError(staged.message);
        return;
      }
      const ids: string[] = [];
      for (const file of staged.files) {
        const createResult = await createSource({
          id: file.suggestedSourceId,
          type: file.fileType,
          path: file.path,
          // owner/ref: no Source field carries this otherwise (KaggleDatasetType.ref's own
          // comment) -- stashed here so a later "Refresh from Kaggle" (refreshKaggleSource
          // mutation) can re-run stage_dataset for the SAME dataset without the user having to
          // re-search it. stage_dataset's own docstring: re-running it is the (v1) refresh
          // mechanism, each file overwritten in place at this exact path.
          federationHintsJson: JSON.stringify({ kaggle_owner: owner, kaggle_ref: ref }),
        });
        if (!createResult.success) {
          throw new Error(`${file.suggestedSourceId}: ${createResult.message}`);
        }
        const registerResult = await registerTable({
          sourceId: file.suggestedSourceId,
          domainId,
          schemaName: "default",
          tableName: file.tableName,
          columns: file.columns.map((c) => ({
            name: c.name,
            visibleTo: roles.map((r) => r.id),
            dataType: c.type,
          })),
        });
        if (!registerResult.success) {
          throw new Error(`${file.suggestedSourceId}: ${registerResult.message}`);
        }
        ids.push(file.suggestedSourceId);
      }
      onSourcesRegistered(ids);
    } catch (e) {
      setRegisterError(e instanceof Error ? e.message : String(e));
    } finally {
      setRegistering(false);
    }
  };

  return (
    <>
      <Select
        required
        label={t("kaggleFormSection.domainLabel")}
        // meta/ops (and the empty-string default) are system domains (domain_policy.py's
        // _SYSTEM_DOMAIN_IDS) -- auto-generated, preserved across config reloads, never a target
        // an end user picks for their own tables.
        data={domains.filter((d) => !d.isSystem).map((d) => ({ value: d.id, label: d.id }))}
        value={domainId}
        onChange={(v) => setDomainId(v ?? "")}
        placeholder={t("kaggleFormSection.domainPlaceholder")}
        style={{ gridColumn: "1 / -1" }}
        data-testid="kaggle-domain-select"
      />
      <TextInput
        required
        label={t("kaggleFormSection.tokenLabel")}
        description={t("kaggleFormSection.tokenDescription")}
        value={token}
        onChange={(e) => {
          setToken(e.target.value);
          setStep("token");
        }}
        placeholder={t("kaggleFormSection.tokenPlaceholder")}
        style={{ gridColumn: "1 / -1" }}
        data-testid="kaggle-token-input"
      />
      <Group style={{ gridColumn: "1 / -1" }} gap="sm" align="center">
        <Button
          type="button"
          onClick={handleValidate}
          loading={validating}
          disabled={!token.trim()}
          data-testid="kaggle-validate-token-button"
        >
          {t("kaggleFormSection.validateButton")}
        </Button>
        {step === "picker" && (
          <Text size="sm" c="teal" data-testid="kaggle-token-valid">
            {t("kaggleFormSection.tokenValid")}
          </Text>
        )}
        {tokenError && (
          <Alert color="red" variant="light" py={4} px="sm" data-testid="kaggle-token-error">
            {tokenError}
          </Alert>
        )}
      </Group>

      {step === "picker" && (
        <>
          <div style={{ gridColumn: "1 / -1" }}>
            <Combobox
              store={combobox}
              onOptionSubmit={(val) => {
                setSelectedRef(val);
                combobox.closeDropdown();
              }}
            >
              <Combobox.Target>
                <TextInput
                  label={t("kaggleFormSection.datasetSearchLabel")}
                  placeholder={t("kaggleFormSection.datasetSearchPlaceholder")}
                  value={query}
                  onChange={(e) => {
                    setQuery(e.target.value);
                    combobox.openDropdown();
                  }}
                  onFocus={() => combobox.openDropdown()}
                  data-testid="kaggle-dataset-search-input"
                />
              </Combobox.Target>
              <Combobox.Dropdown>
                <Combobox.Options data-testid="kaggle-dataset-options">
                  {datasets.length === 0 ? (
                    <Combobox.Empty>{t("kaggleFormSection.noDatasetsFound")}</Combobox.Empty>
                  ) : (
                    datasets.map((d) => (
                      <Combobox.Option
                        value={d.ref}
                        key={d.ref}
                        data-testid={`kaggle-dataset-${d.ref}`}
                      >
                        <Text size="sm" fw={500}>
                          {d.title}
                        </Text>
                        <Text size="xs" c="dimmed">
                          {d.subtitle}
                        </Text>
                      </Combobox.Option>
                    ))
                  )}
                </Combobox.Options>
              </Combobox.Dropdown>
            </Combobox>
          </div>

          {selectedRef && (
            <Group style={{ gridColumn: "1 / -1" }} gap="sm" align="center">
              <Text size="sm" data-testid="kaggle-selected-dataset">
                {t("kaggleFormSection.selectedDataset", { ref: selectedRef })}
              </Text>
              <Button
                type="button"
                onClick={handleConfirmDataset}
                loading={registering}
                disabled={!domainId}
                data-testid="kaggle-register-button"
              >
                {t("kaggleFormSection.registerButton")}
              </Button>
            </Group>
          )}
          {registerError && (
            <Alert color="red" variant="light" py={4} px="sm" data-testid="kaggle-register-error">
              {registerError}
            </Alert>
          )}
        </>
      )}
    </>
  );
}
