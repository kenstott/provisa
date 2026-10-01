// Copyright (c) 2026 Kenneth Stott
// Canary: 0c7d3a95-6b21-4e58-a4f9-1d8e5b2c7a60
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Group,
  NativeSelect,
  NumberInput,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { fetchOrgs, type Org } from "../../api/admin";
import {
  fetchDebugTrace,
  setDebugHintRole,
  startDebugWindow,
  stopDebugWindow,
  type DebugTraceScope,
  type DebugTraceState,
} from "../../api/debugTrace";

/**
 * REQ-1910: the operator's control over debug tracing.
 *
 * Tracing is lean by default. Here the operator turns debug detail on for an org, a role or a
 * source for a stated number of minutes, sees the open windows counting down, stops one early,
 * and names the roles whose requests may ask for a debug trace of themselves. Rendered only for a
 * holder of `platform_settings` (it sits in the deployment-wide half of the Observability page).
 */

const DEFAULT_MINUTES = 15;
const SCOPES: DebugTraceScope[] = ["org", "role", "source"];
const SCOPE_LABEL: Record<DebugTraceScope, string> = {
  org: "debugTracePanel.scopeOrg",
  role: "debugTracePanel.scopeRole",
  source: "debugTracePanel.scopeSource",
};

function formatRemaining(seconds: number): string {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = String(seconds % 60).padStart(2, "0");
  return hours > 0 ? `${hours}:${String(minutes).padStart(2, "0")}:${secs}` : `${minutes}:${secs}`;
}

export function DebugTracePanel() {
  const { t } = useTranslation();
  // The server's answer and the moment it arrived: time remaining is the server's figure minus
  // the seconds that have passed here since, so the countdown does not depend on this clock
  // agreeing with the server's.
  const [loaded, setLoaded] = useState<{ state: DebugTraceState; at: number } | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const [orgs, setOrgs] = useState<Org[]>([]);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const [scope, setScope] = useState<DebugTraceScope>("org");
  const [orgId, setOrgId] = useState("");
  const [target, setTarget] = useState("");
  const [minutes, setMinutes] = useState<number>(DEFAULT_MINUTES);

  const [hintOrgId, setHintOrgId] = useState("");
  const [hintRole, setHintRole] = useState("");

  const accept = useCallback((state: DebugTraceState) => {
    const at = Date.now();
    setLoaded({ state, at });
    setNow(at);
  }, []);

  useEffect(() => {
    fetchDebugTrace()
      .then(accept)
      .catch((e: Error) => setError(e.message || t("debugTracePanel.loadFailed")));
    fetchOrgs()
      .then((list) => {
        setOrgs(list);
        if (list.length > 0) {
          setOrgId((current) => current || list[0].id);
          setHintOrgId((current) => current || list[0].id);
        }
      })
      .catch((e: Error) => setError(e.message || t("debugTracePanel.loadFailed")));
  }, [accept, t]);

  const elapsed = loaded ? Math.floor((now - loaded.at) / 1000) : 0;
  const windows = (loaded?.state.windows ?? [])
    .map((w) => ({ ...w, remaining: w.remaining_seconds - elapsed }))
    .filter((w) => w.remaining > 0);
  const counting = windows.length > 0;

  useEffect(() => {
    if (!counting) return;
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [counting]);

  const run = async (call: () => Promise<DebugTraceState>) => {
    setBusy(true);
    setError("");
    try {
      accept(await call());
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const trimmedTarget = target.trim();
  const canStart = orgId !== "" && minutes >= 1 && (scope === "org" || trimmedTarget !== "");
  const start = () =>
    run(() =>
      startDebugWindow({
        scope,
        org_id: orgId,
        target: scope === "org" ? null : trimmedTarget,
        minutes,
      }),
    );

  const trimmedHintRole = hintRole.trim();
  const permit = () =>
    run(async () => {
      const next = await setDebugHintRole({
        org_id: hintOrgId,
        role_id: trimmedHintRole,
        permitted: true,
      });
      setHintRole("");
      return next;
    });

  const orgOptions = orgs.map((o) => ({
    value: o.id,
    label: o.name ? `${o.name} (${o.id})` : o.id,
  }));
  const hintRoles = loaded?.state.hint_roles ?? [];
  const maxMinutes = loaded?.state.max_minutes;

  return (
    <Stack gap="md" data-testid="debug-trace-panel">
      <Text c="dimmed" size="sm">
        {t("debugTracePanel.intro")}
      </Text>

      {error && (
        <Alert color="red" data-testid="debug-trace-error">
          {error}
        </Alert>
      )}

      <Title order={5}>{t("debugTracePanel.startTitle")}</Title>
      <Group align="end" wrap="wrap">
        <NativeSelect
          label={t("debugTracePanel.scopeLabel")}
          data={SCOPES.map((s) => ({ value: s, label: t(SCOPE_LABEL[s]) }))}
          value={scope}
          onChange={(e) => setScope(e.currentTarget.value as DebugTraceScope)}
          data-testid="debug-trace-scope"
        />
        <NativeSelect
          label={t("debugTracePanel.orgLabel")}
          data={orgOptions}
          value={orgId}
          onChange={(e) => setOrgId(e.currentTarget.value)}
          data-testid="debug-trace-org"
        />
        {scope !== "org" && (
          <TextInput
            label={t(
              scope === "role" ? "debugTracePanel.roleLabel" : "debugTracePanel.sourceLabel",
            )}
            value={target}
            onChange={(e) => setTarget(e.currentTarget.value)}
            data-testid="debug-trace-target"
          />
        )}
        <NumberInput
          label={t("debugTracePanel.minutesLabel")}
          description={
            maxMinutes === undefined
              ? undefined
              : t("debugTracePanel.minutesHelp", { max: maxMinutes })
          }
          min={1}
          max={maxMinutes}
          allowDecimal={false}
          value={minutes}
          onChange={(v) => setMinutes(typeof v === "number" ? v : 0)}
          data-testid="debug-trace-minutes"
        />
        <Button onClick={start} disabled={busy || !canStart} data-testid="debug-trace-start">
          {t("debugTracePanel.start")}
        </Button>
      </Group>

      <Title order={5}>{t("debugTracePanel.activeTitle")}</Title>
      {loaded && windows.length === 0 && (
        <Text c="dimmed" size="sm" data-testid="debug-trace-no-windows">
          {t("debugTracePanel.noWindows")}
        </Text>
      )}
      {windows.length > 0 && (
        <Table data-testid="debug-trace-windows">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("debugTracePanel.colScope")}</Table.Th>
              <Table.Th>{t("debugTracePanel.colOrg")}</Table.Th>
              <Table.Th>{t("debugTracePanel.colTarget")}</Table.Th>
              <Table.Th>{t("debugTracePanel.colRemaining")}</Table.Th>
              <Table.Th>{t("debugTracePanel.colStartedBy")}</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {windows.map((w) => (
              <Table.Tr key={w.id} data-testid={`debug-trace-window-${w.id}`}>
                <Table.Td>{t(SCOPE_LABEL[w.scope])}</Table.Td>
                <Table.Td>{w.org_id}</Table.Td>
                <Table.Td>{w.target ?? t("debugTracePanel.wholeOrg")}</Table.Td>
                <Table.Td data-testid={`debug-trace-remaining-${w.id}`}>
                  {formatRemaining(w.remaining)}
                </Table.Td>
                <Table.Td>{w.created_by ?? "—"}</Table.Td>
                <Table.Td>
                  <Button
                    size="xs"
                    variant="default"
                    disabled={busy}
                    onClick={() => run(() => stopDebugWindow(w.id))}
                    data-testid={`debug-trace-stop-${w.id}`}
                  >
                    {t("debugTracePanel.stop")}
                  </Button>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}

      <Title order={5}>{t("debugTracePanel.hintTitle")}</Title>
      <Text c="dimmed" size="sm">
        {t("debugTracePanel.hintIntro")}
      </Text>
      <Group align="end" wrap="wrap">
        <NativeSelect
          label={t("debugTracePanel.orgLabel")}
          data={orgOptions}
          value={hintOrgId}
          onChange={(e) => setHintOrgId(e.currentTarget.value)}
          data-testid="debug-trace-hint-org"
        />
        <TextInput
          label={t("debugTracePanel.roleLabel")}
          value={hintRole}
          onChange={(e) => setHintRole(e.currentTarget.value)}
          data-testid="debug-trace-hint-role"
        />
        <Button
          onClick={permit}
          disabled={busy || hintOrgId === "" || trimmedHintRole === ""}
          data-testid="debug-trace-hint-permit"
        >
          {t("debugTracePanel.permit")}
        </Button>
      </Group>
      {loaded && hintRoles.length === 0 && (
        <Text c="dimmed" size="sm" data-testid="debug-trace-no-hint-roles">
          {t("debugTracePanel.noHintRoles")}
        </Text>
      )}
      {hintRoles.length > 0 && (
        <Table data-testid="debug-trace-hint-roles">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("debugTracePanel.colOrg")}</Table.Th>
              <Table.Th>{t("debugTracePanel.roleLabel")}</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {hintRoles.map((r) => (
              <Table.Tr key={`${r.org_id}/${r.role_id}`}>
                <Table.Td>{r.org_id}</Table.Td>
                <Table.Td>{r.role_id}</Table.Td>
                <Table.Td>
                  <Button
                    size="xs"
                    variant="default"
                    disabled={busy}
                    onClick={() =>
                      run(() =>
                        setDebugHintRole({
                          org_id: r.org_id,
                          role_id: r.role_id,
                          permitted: false,
                        }),
                      )
                    }
                    data-testid={`debug-trace-hint-revoke-${r.org_id}-${r.role_id}`}
                  >
                    {t("debugTracePanel.revoke")}
                  </Button>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}
    </Stack>
  );
}
