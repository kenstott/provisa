// Copyright (c) 2026 Kenneth Stott
// Canary: 5a20e6c9-71bd-4f3c-8e42-0d9b6c7a1f54
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Card, Group, Stack, Text, Textarea, TextInput, Title } from "@mantine/core";
import {
  fetchMaintenanceNotice,
  setMaintenanceNotice,
  type MaintenanceNotice,
} from "../../api/maintenance";
import { shutdownAllServices } from "../../api/lifecycle";
import { ConfirmDialog } from "../ConfirmDialog";

/**
 * REQ-1466: the platform admin's control over the scheduled-downtime banner.
 *
 * Turning it on is the first step of any planned window — the engine-cluster topology switch
 * (REQ-1465) replaces the cluster and every shard on it, so queries fail for the duration — and
 * turning it off is the last. Gated on `platform_settings`, the deployment-wide right, because the
 * subject is the deployment rather than any one org.
 *
 * The message field is left empty by default: an empty message means the server's standard wording,
 * so the deployment says the same thing every time without an administrator having to compose it.
 */
import { ConfigFileSection } from "./settingsCards";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";

export function MaintenanceTab() {
  const { t } = useTranslation();
  const [notice, setNotice] = useState<MaintenanceNotice | null>(null);
  const [message, setMessage] = useState("");
  // A `datetime-local` value ("2026-08-14T22:30"), which the browser reads in the operator's own
  // zone. Converted to an instant on submit so the banner is not showing one person's wall clock to
  // everyone else.
  const [endsAt, setEndsAt] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [stopBusy, setStopBusy] = useState(false);
  const [stopError, setStopError] = useState("");

  const load = useCallback(() => {
    fetchMaintenanceNotice()
      .then((next) => {
        setNotice(next);
        if (next.active) {
          setMessage(next.message ?? "");
          setEndsAt(next.ends_at ? new Date(next.ends_at).toISOString().slice(0, 16) : "");
        }
      })
      .catch((e: Error) => setError(e.message));
  }, []);

  useEffect(load, [load]);

  const apply = async (active: boolean) => {
    setBusy(true);
    setError("");
    try {
      const next = await setMaintenanceNotice({
        active,
        message: active && message.trim() ? message.trim() : null,
        ends_at: active && endsAt ? new Date(endsAt).toISOString() : null,
      });
      setNotice(next);
      if (!active) {
        setMessage("");
        setEndsAt("");
      }
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const stopAllServices = async () => {
    setStopBusy(true);
    setStopError("");
    try {
      await shutdownAllServices();
    } catch (e) {
      setStopError((e as Error).message);
    } finally {
      setStopBusy(false);
    }
  };

  return (
    <Stack gap="md">
      <div>
        <Title order={3}>{t("maintenanceTab.title")}</Title>
        <Text c="dimmed" size="sm">
          {t("maintenanceTab.intro")}
        </Text>
      </div>

      {error && (
        <Alert color="red" data-testid="maintenance-error">
          {error}
        </Alert>
      )}

      {notice?.active && (
        <Alert color="orange" data-testid="maintenance-active">
          {notice.started_at
            ? t("maintenanceTab.liveSince", {
                time: new Date(notice.started_at).toLocaleString(),
              })
            : t("maintenanceTab.live")}
        </Alert>
      )}

      <Card withBorder padding="md">
        <FormLayout>
          <FormField hint={t("maintenanceTab.messageHint")} size="full">
            <Textarea
              label={t("maintenanceTab.messageLabel")}
              placeholder={t("maintenanceTab.messagePlaceholder")}
              autosize
              minRows={2}
              value={message}
              onChange={(e) => setMessage(e.currentTarget.value)}
              data-testid="maintenance-message"
            />
          </FormField>
          <FormField hint={t("maintenanceTab.endsAtHint")} size="medium">
            <TextInput
              type="datetime-local"
              label={t("maintenanceTab.endsAtLabel")}
              value={endsAt}
              onChange={(e) => setEndsAt(e.currentTarget.value)}
              data-testid="maintenance-ends-at"
            />
          </FormField>
          <FormFooter>
            <Button
              color="orange"
              loading={busy}
              onClick={() => apply(true)}
              data-testid="maintenance-on"
            >
              {notice?.active ? t("maintenanceTab.updateBanner") : t("maintenanceTab.showBanner")}
            </Button>
            <Button
              variant="default"
              disabled={busy || !notice?.active}
              onClick={() => apply(false)}
              data-testid="maintenance-off"
            >
              {t("maintenanceTab.clearBanner")}
            </Button>
          </FormFooter>
        </FormLayout>
      </Card>

      {/* REQ-1349: exporting, diffing and re-applying the config file is deployment maintenance,
          not an overview statistic. */}
      <ConfigFileSection />

      <div>
        <Title order={3} c="red">
          {t("maintenanceTab.dangerTitle")}
        </Title>
        <Text c="dimmed" size="sm">
          {t("maintenanceTab.dangerIntro")}
        </Text>
      </div>

      {stopError && (
        <Alert color="red" data-testid="stop-all-services-error">
          {stopError}
        </Alert>
      )}

      <Card withBorder padding="md">
        <Group>
          <ConfirmDialog
            title={t("maintenanceTab.stopTitle")}
            consequence={t("maintenanceTab.stopConsequence")}
            confirmLabel={t("common.stop")}
            onConfirm={stopAllServices}
            testId="stop-all-services-dialog"
          >
            {(openConfirm) => (
              <Button
                color="red"
                variant="light"
                loading={stopBusy}
                onClick={openConfirm}
                data-testid="stop-all-services"
              >
                {t("maintenanceTab.stopAll")}
              </Button>
            )}
          </ConfirmDialog>
        </Group>
      </Card>
    </Stack>
  );
}
