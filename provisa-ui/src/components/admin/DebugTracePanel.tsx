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
import { Alert, Button, NumberInput, Stack, Table, Text, TextInput, Title } from "@mantine/core";
import { fetchOrgs, type Org } from "../../api/admin";
import {
  fetchDebugTrace,
  setDebugHintRole,
  startDebugWindow,
  stopDebugWindow,
  type DebugTraceScope,
  type DebugTraceState,
} from "../../api/debugTrace";
import { FieldGroup, FormField, FormLayout } from "../form/FormLayout";

/**
 * REQ-1910: the operator's control over debug tracing.
 *
 * Tracing is lean by default. Debug detail is turned on for a stated number of minutes, the open
 * windows count down, and one can be stopped early. REQ-1327 divides it by layer, as the server
 * does. Without `orgId` this is the platform's panel: a window over any org as a whole. With
 * `orgId` it is that org's administrator's: a window naming a role or source in the org, and the
 * roles whose requests may ask for a debug trace of themselves. An org-wide window of its org is
 * listed to the org, and only the platform stops it.
 */

const DEFAULT_MINUTES = 15;
const ORG_SCOPES: DebugTraceScope[] = ["role", "source"];
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

export function DebugTracePanel({ orgId: ownOrg }: { orgId?: string }) {
  const { t } = useTranslation();
  const platform = ownOrg === undefined;
  // The server's answer and the moment it arrived: time remaining is the server's figure minus
  // the seconds that have passed here since, so the countdown does not depend on this clock
  // agreeing with the server's.
  const [loaded, setLoaded] = useState<{ state: DebugTraceState; at: number } | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const [orgs, setOrgs] = useState<Org[]>([]);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const [scope, setScope] = useState<DebugTraceScope>(platform ? "org" : "role");
  const [orgId, setOrgId] = useState(ownOrg ?? "");
  const [target, setTarget] = useState("");
  const [minutes, setMinutes] = useState<number>(DEFAULT_MINUTES);

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
    if (!platform) return;
    fetchOrgs()
      .then((list) => {
        setOrgs(list);
        if (list.length > 0) setOrgId((current) => current || list[0].id);
      })
      .catch((e: Error) => setError(e.message || t("debugTracePanel.loadFailed")));
  }, [accept, platform, t]);

  const elapsed = loaded ? Math.floor((now - loaded.at) / 1000) : 0;
  const windows = (loaded?.state.windows ?? [])
    // The server lists a caller both reaches together; each panel shows its own layer's.
    .filter((w) => (platform ? w.scope === "org" : w.org_id === ownOrg))
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
        org_id: orgId,
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
  const hintRoles = (loaded?.state.hint_roles ?? []).filter((r) => r.org_id === ownOrg);
  const maxMinutes = loaded?.state.max_minutes;

  return (
    <Stack gap="md" data-testid="debug-trace-panel">
      <Text c="dimmed" size="sm">
        {t(platform ? "debugTracePanel.introPlatform" : "debugTracePanel.introOrg")}
      </Text>

      {error && (
        <Alert color="red" data-testid="debug-trace-error">
          {error}
        </Alert>
      )}

      <Title order={5}>{t("debugTracePanel.startTitle")}</Title>
      <FormLayout>
        <FieldGroup row>
          {!platform && (
            <FormField size="medium">
              <label>
                {t("debugTracePanel.scopeLabel")}
                <select
                  className="form-select"
                  value={scope}
                  onChange={(e) => setScope(e.currentTarget.value as DebugTraceScope)}
                  data-testid="debug-trace-scope"
                >
                  {ORG_SCOPES.map((s) => ({ value: s, label: t(SCOPE_LABEL[s]) })).map((o) => (
                    <option key={o.value} value={o.value}>
                      {o.label}
                    </option>
                  ))}
                </select>
              </label>
            </FormField>
          )}
          {platform && (
            <FormField size="medium">
              <label>
                {t("debugTracePanel.orgLabel")}
                <select
                  className="form-select"
                  value={orgId}
                  onChange={(e) => setOrgId(e.currentTarget.value)}
                  data-testid="debug-trace-org"
                >
                  {orgOptions.map((o) => (
                    <option key={o.value} value={o.value}>
                      {o.label}
                    </option>
                  ))}
                </select>
              </label>
            </FormField>
          )}
          {scope !== "org" && (
            <FormField size="medium">
              <TextInput
                label={t(
                  scope === "role" ? "debugTracePanel.roleLabel" : "debugTracePanel.sourceLabel",
                )}
                value={target}
                onChange={(e) => setTarget(e.currentTarget.value)}
                data-testid="debug-trace-target"
              />
            </FormField>
          )}
          <FormField
            hint={
              maxMinutes === undefined
                ? undefined
                : t("debugTracePanel.minutesHelp", { max: maxMinutes })
            }
            size="short"
          >
            <NumberInput
              label={t("debugTracePanel.minutesLabel")}
              min={1}
              max={maxMinutes}
              allowDecimal={false}
              value={minutes}
              onChange={(v) => setMinutes(typeof v === "number" ? v : 0)}
              data-testid="debug-trace-minutes"
            />
          </FormField>
          <Button onClick={start} disabled={busy || !canStart} data-testid="debug-trace-start">
            {t("debugTracePanel.start")}
          </Button>
        </FieldGroup>
      </FormLayout>

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
              {platform && <Table.Th>{t("debugTracePanel.colOrg")}</Table.Th>}
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
                {platform && <Table.Td>{w.org_id}</Table.Td>}
                <Table.Td>{w.target ?? t("debugTracePanel.wholeOrg")}</Table.Td>
                <Table.Td data-testid={`debug-trace-remaining-${w.id}`}>
                  {formatRemaining(w.remaining)}
                </Table.Td>
                <Table.Td>{w.created_by ?? "—"}</Table.Td>
                <Table.Td>
                  {(platform || w.scope !== "org") && (
                    <Button
                      size="xs"
                      variant="default"
                      disabled={busy}
                      onClick={() => run(() => stopDebugWindow(w.id))}
                      data-testid={`debug-trace-stop-${w.id}`}
                    >
                      {t("debugTracePanel.stop")}
                    </Button>
                  )}
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}

      {/* REQ-1327: a hint names a role in an org, so it is that org's administrator's. */}
      {!platform && (
        <>
        <Title order={5}>{t("debugTracePanel.hintTitle")}</Title>
        <Text c="dimmed" size="sm">
          {t("debugTracePanel.hintIntro")}
        </Text>
        <FormLayout>
          <FieldGroup row>
            <FormField size="medium">
              <TextInput
                label={t("debugTracePanel.roleLabel")}
                value={hintRole}
                onChange={(e) => setHintRole(e.currentTarget.value)}
                data-testid="debug-trace-hint-role"
              />
            </FormField>
            <Button
              onClick={permit}
              disabled={busy || trimmedHintRole === ""}
              data-testid="debug-trace-hint-permit"
            >
              {t("debugTracePanel.permit")}
            </Button>
          </FieldGroup>
        </FormLayout>
        {loaded && hintRoles.length === 0 && (
          <Text c="dimmed" size="sm" data-testid="debug-trace-no-hint-roles">
            {t("debugTracePanel.noHintRoles")}
          </Text>
        )}
        {hintRoles.length > 0 && (
          <Table data-testid="debug-trace-hint-roles">
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("debugTracePanel.roleLabel")}</Table.Th>
                <Table.Th />
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {hintRoles.map((r) => (
                <Table.Tr key={`${r.org_id}/${r.role_id}`}>
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
        </>
      )}
    </Stack>
  );
}
