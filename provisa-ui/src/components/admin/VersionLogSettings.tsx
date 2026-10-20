// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109: a table's version log is captured on its own refresh interval, and reads a row
// committed late within its allowed lateness. Both are set here, on the log's row, beside the
// full capture that reads the whole table again.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Group, NumberInput, Popover, Stack } from "@mantine/core";
import type { MVInfo } from "../../api/admin";
import { useUpdateVersionLog } from "../../hooks/useAdminOpsQueries";
import { serverMessage } from "../../i18n/serverMessage";

export function VersionLogSettings({
  log,
  capturing,
  onFullCapture,
}: {
  log: MVInfo;
  /** A capture of this log is under way. */
  capturing: boolean;
  /** Read the whole table again, recording the rows it deleted. */
  onFullCapture: () => void;
}) {
  const { t } = useTranslation();
  const { updateVersionLog } = useUpdateVersionLog();
  const [opened, setOpened] = useState(false);
  const [interval, setInterval] = useState<number>(log.refreshInterval);
  const [lateness, setLateness] = useState<number>(log.allowedLateness);
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState("");

  const open = () => {
    setInterval(log.refreshInterval);
    setLateness(log.allowedLateness);
    setProblem("");
    setOpened(true);
  };
  const save = async () => {
    setSaving(true);
    setProblem("");
    try {
      const result = await updateVersionLog(log.id, interval, lateness);
      if (!result.success) {
        setProblem(serverMessage(result, result.message));
        return;
      }
      setOpened(false);
    } catch (e) {
      setProblem(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Popover opened={opened} onChange={setOpened} position="bottom-end" withArrow>
      <Popover.Target>
        <Button
          size="xs"
          variant="subtle"
          onClick={() => (opened ? setOpened(false) : open())}
          data-testid={`mv-log-settings-btn-${log.id}`}
        >
          {t("cacheManager.materialized.logSettings")}
        </Button>
      </Popover.Target>
      <Popover.Dropdown>
        <Stack gap="xs" w={260}>
          <NumberInput
            label={t("cacheManager.materialized.logInterval")}
            description={t("cacheManager.materialized.logIntervalHelp")}
            min={1}
            allowDecimal={false}
            value={interval}
            onChange={(v) => setInterval(Number(v))}
            data-testid={`mv-log-interval-${log.id}`}
          />
          <NumberInput
            label={t("cacheManager.materialized.logLateness")}
            description={t("cacheManager.materialized.logLatenessHelp")}
            min={0}
            value={lateness}
            onChange={(v) => setLateness(Number(v))}
            data-testid={`mv-log-lateness-${log.id}`}
          />
          {problem && <Alert color="red">{problem}</Alert>}
          <Group justify="space-between">
            <Button
              size="xs"
              variant="light"
              onClick={onFullCapture}
              disabled={capturing}
              title={t("cacheManager.materialized.fullCaptureHelp")}
              data-testid={`mv-full-capture-btn-${log.id}`}
            >
              {t("cacheManager.materialized.fullCapture")}
            </Button>
            <Button
              size="xs"
              onClick={save}
              loading={saving}
              data-testid={`mv-log-save-${log.id}`}
            >
              {t("cacheManager.materialized.logSave")}
            </Button>
          </Group>
        </Stack>
      </Popover.Dropdown>
    </Popover>
  );
}
