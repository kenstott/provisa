// Copyright (c) 2026 Kenneth Stott
// Canary: 030c887e-c3c2-4497-810e-bbd5cb30160f
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
import { Button, NumberInput, Popover, Stack } from "@mantine/core";
import type { MVInfo } from "../../api/admin";
import { useUpdateVersionLog } from "../../hooks/useAdminOpsQueries";
import { serverMessage } from "../../i18n/serverMessage";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { Tip } from "../Tip";

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
          <FormLayout>
            <FormField size="short" hint={t("cacheManager.materialized.logIntervalHelp")}>
              <NumberInput
                label={t("cacheManager.materialized.logInterval")}
                min={1}
                allowDecimal={false}
                value={interval}
                onChange={(v) => setInterval(Number(v))}
                data-testid={`mv-log-interval-${log.id}`}
              />
            </FormField>
            <FormField size="short" help={t("cacheManager.materialized.logLatenessHelp")}>
              <NumberInput
                label={t("cacheManager.materialized.logLateness")}
                min={0}
                value={lateness}
                onChange={(v) => setLateness(Number(v))}
                data-testid={`mv-log-lateness-${log.id}`}
              />
            </FormField>
            <FormFooter
              onSave={save}
              onCancel={() => setOpened(false)}
              saving={saving}
              saveProps={{ "data-testid": `mv-log-save-${log.id}` }}
              problem={problem}
            >
              <Tip text={t("cacheManager.materialized.fullCaptureHelp")}>
                <Button
                  size="xs"
                  variant="light"
                  onClick={onFullCapture}
                  disabled={capturing}
                  data-testid={`mv-full-capture-btn-${log.id}`}
                >
                  {t("cacheManager.materialized.fullCapture")}
                </Button>
              </Tip>
            </FormFooter>
          </FormLayout>
        </Stack>
      </Popover.Dropdown>
    </Popover>
  );
}
