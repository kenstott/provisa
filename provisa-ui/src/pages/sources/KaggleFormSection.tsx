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
import { Alert, Button, Combobox, Group, Text, TextInput, useCombobox } from "@mantine/core";
import {
  useCreateSource,
  useKaggleDatasetsLazy,
  useKaggleTokenValidLazy,
  useStageKaggleDataset,
} from "../../hooks/useAdminQueries";

// REQ-1783: Kaggle's own dataset search requires auth, so the form cannot present token entry
// and dataset search at once — the picker step only exists once step 1 validates live.
type Step = "token" | "picker";

interface KaggleFormSectionProps {
  // The shared "ID" field above this section (SourcesPage.tsx's form.id) -- previously ignored
  // entirely: every Kaggle-staged source got an opaque kg_<timestamp>_<dataset-ref> id regardless
  // of what the user typed there, which they'd naturally expect to be honored (confirmed live:
  // typed "c", got "kg_1789848538582_spotify-global-chart-totals-and-lyrics_data" instead).
  sourceIdHint?: string;
  onSourcesRegistered: (sourceIds: string[]) => void;
}

export function KaggleFormSection({ sourceIdHint, onSourcesRegistered }: KaggleFormSectionProps) {
  const { t } = useTranslation();
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
      // Honor the ID the user actually typed above; fall back to an opaque kg_<timestamp>_<ref>
      // scheme only when the field was left blank -- the outer form's `required` on that input
      // never actually gates this section's own "Add Dataset" button (type="button", bypasses
      // native HTML5 validation).
      const idPrefix =
        sourceIdHint?.trim() || `kg_${Date.now()}_${ref.replace(/[^a-zA-Z0-9_-]/g, "_")}`;
      const staged = await stageKaggleDataset(token, owner, ref, idPrefix);
      if (!staged.success) {
        setRegisterError(staged.message);
        return;
      }
      // (Amended 2026-09-19) ONE `files`-type Source for the whole staged directory, single- or
      // multi-file dataset alike -- the pgwire-file connector already discovers every file in a
      // directory as its own table, recursively (REQ-1690), so there is nothing left for this
      // form to enumerate per file. Only the SOURCE is created here; registering its table(s)
      // (domain, alias, columns) is left to the normal Register Table screen, exactly like every
      // other connector. Registering eagerly here (the pre-amendment design: one plain csv/parquet
      // Source PER FILE, auto-registered) is what previously turned a same-named-table collision
      // into a dead end with no alias field to fix it, and cluttered the Sources list with one row
      // per file in an 11-file Kaggle dataset instead of one coherent source.
      const createResult = await createSource({
        id: staged.suggestedSourceId,
        type: "files",
        path: staged.directory,
        // owner/ref: no Source field carries this otherwise (KaggleDatasetType.ref's own
        // comment) -- stashed here so a later "Refresh from Kaggle" (refreshKaggleSource
        // mutation) can re-run stage_dataset for the SAME dataset without the user having to
        // re-search it. stage_dataset's own docstring: re-running it is the (v1) refresh
        // mechanism, each file overwritten in place at this exact path.
        federationHintsJson: JSON.stringify({ kaggle_owner: owner, kaggle_ref: ref }),
      });
      if (!createResult.success) {
        throw new Error(`${staged.suggestedSourceId}: ${createResult.message}`);
      }
      onSourcesRegistered([staged.suggestedSourceId]);
    } catch (e) {
      setRegisterError(e instanceof Error ? e.message : String(e));
    } finally {
      setRegistering(false);
    }
  };

  return (
    <>
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
