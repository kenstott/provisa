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
import { Alert, Button, Card, Group, Stack, Text, Textarea, TextInput, Title } from "@mantine/core";
import {
  fetchMaintenanceNotice,
  setMaintenanceNotice,
  type MaintenanceNotice,
} from "../../api/maintenance";

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

export function MaintenanceTab() {
  const [notice, setNotice] = useState<MaintenanceNotice | null>(null);
  const [message, setMessage] = useState("");
  // A `datetime-local` value ("2026-08-14T22:30"), which the browser reads in the operator's own
  // zone. Converted to an instant on submit so the banner is not showing one person's wall clock to
  // everyone else.
  const [endsAt, setEndsAt] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

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

  return (
    <Stack gap="md">
      <div>
        <Title order={3}>Scheduled maintenance</Title>
        <Text c="dimmed" size="sm">
          Shows a banner to every signed-in user, on every page, until you turn it off. Turn it on
          before planned work that interrupts queries — replacing the engine cluster, switching its
          topology, or upgrading the control plane — so the interruption reads as scheduled rather
          than broken.
        </Text>
      </div>

      {error && (
        <Alert color="red" data-testid="maintenance-error">
          {error}
        </Alert>
      )}

      {notice?.active && (
        <Alert color="orange" data-testid="maintenance-active">
          The banner is live
          {notice.started_at ? ` since ${new Date(notice.started_at).toLocaleString()}` : ""}.
        </Alert>
      )}

      <Card withBorder padding="md">
        <Stack gap="sm">
          <Textarea
            label="Message"
            description="Leave empty for the standard wording."
            placeholder="Provisa is undergoing scheduled maintenance…"
            autosize
            minRows={2}
            value={message}
            onChange={(e) => setMessage(e.currentTarget.value)}
            data-testid="maintenance-message"
          />
          <TextInput
            type="datetime-local"
            label="Expected back by"
            description="Leave empty to say no estimate is being offered rather than to imply one."
            value={endsAt}
            onChange={(e) => setEndsAt(e.currentTarget.value)}
            data-testid="maintenance-ends-at"
          />
          <Group>
            <Button
              color="orange"
              loading={busy}
              onClick={() => apply(true)}
              data-testid="maintenance-on"
            >
              {notice?.active ? "Update banner" : "Show banner"}
            </Button>
            <Button
              variant="default"
              disabled={busy || !notice?.active}
              onClick={() => apply(false)}
              data-testid="maintenance-off"
            >
              Clear banner
            </Button>
          </Group>
        </Stack>
      </Card>

      {/* REQ-1349: exporting, diffing and re-applying the config file is deployment maintenance,
          not an overview statistic. */}
      <ConfigFileSection />
    </Stack>
  );
}
