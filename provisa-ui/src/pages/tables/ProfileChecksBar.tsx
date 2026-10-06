// Copyright (c) 2026 Kenneth Stott
// Canary: 41c7e9a2-8b05-4f63-9d1e-c3a6b0f8e274
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Group, Select } from "@mantine/core";
import {
  createDriftCheck,
  createExpectationCheck,
  fetchProfileChecks,
  type ExportResult,
  type ProfileCheckCandidates,
} from "../../api/profiler";

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

// REQ-1934 EXCEPTIONS COME FROM CHECKERS: the profiler raises nothing itself. Create drift check
// adds "the latest run has no drifting measure" to a checker scanning the registered drift table;
// Create expectation check adds "the latest run's measures lie within the expectation whose period
// holds the run" for a registered table of the expectations shape. The server refuses either by
// name where no checker can take it.
export function ProfileChecksBar({ tableId }: { tableId: number }) {
  const { t } = useTranslation();
  const [candidates, setCandidates] = useState<ProfileCheckCandidates | null>(null);
  const [checker, setChecker] = useState<string | null>(null);
  const [expectations, setExpectations] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ ok: boolean; text: string } | null>(null);

  const load = () => {
    if (candidates != null) return;
    fetchProfileChecks(tableId)
      .then(setCandidates)
      .catch((e: unknown) => setNotice({ ok: false, text: message(e) }));
  };

  const report = (r: ExportResult) =>
    setNotice({
      ok: true,
      text: r.added
        ? t("profileChecksBar.added", { table: r.checkerTable.tableName })
        : t("profileChecksBar.already", { table: r.checkerTable.tableName }),
    });

  const chosen = checker == null ? null : Number(checker);
  const drift = () => {
    load();
    createDriftCheck(tableId, chosen)
      .then(report)
      .catch((e: unknown) => setNotice({ ok: false, text: message(e) }));
  };
  const expectation = () =>
    expectations != null &&
    createExpectationCheck(tableId, Number(expectations), chosen)
      .then(report)
      .catch((e: unknown) => setNotice({ ok: false, text: message(e) }));

  return (
    <>
      <Group gap="xs" mb="xs" align="end">
        {candidates != null && candidates.checkers.length > 1 && (
          <Select
            size="xs"
            label={t("profileChecksBar.checkerLabel")}
            data={candidates.checkers.map((c) => ({
              value: String(c.id),
              label: `${c.tableName} (${c.checker})`,
            }))}
            value={checker}
            onChange={setChecker}
            data-testid="profile-checks-checker"
          />
        )}
        <Button size="xs" variant="light" onClick={drift} data-testid="create-drift-check">
          {t("profileChecksBar.driftCheck")}
        </Button>
        <Select
          size="xs"
          label={t("profileChecksBar.expectationsLabel")}
          placeholder={t("profileChecksBar.expectationsPlaceholder")}
          data={(candidates?.expectationTables ?? []).map((e) => ({
            value: String(e.id),
            label: e.published,
          }))}
          value={expectations}
          onChange={setExpectations}
          onDropdownOpen={load}
          nothingFoundMessage={t("profileChecksBar.noExpectations")}
          data-testid="profile-checks-expectations"
        />
        <Button
          size="xs"
          variant="light"
          disabled={expectations == null}
          onClick={expectation}
          data-testid="create-expectation-check"
        >
          {t("profileChecksBar.expectationCheck")}
        </Button>
      </Group>
      {notice && (
        <Alert color={notice.ok ? "green" : "red"} mb="xs" data-testid="profile-checks-notice">
          {notice.text}
        </Alert>
      )}
    </>
  );
}
