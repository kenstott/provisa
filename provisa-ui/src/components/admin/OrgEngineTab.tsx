// Copyright (c) 2026 Kenneth Stott
// Canary: 7a41f0c9-63de-4b52-8a7d-04c9d6b1e2f3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Stack, Text } from "@mantine/core";
import { Link } from "react-router-dom";
import { fetchOrgEngine, type OrgEngineState } from "../../api/admin";
import { RegionEngineTabs } from "./RegionEngineTabs";
import { useRegionEngines } from "./useRegionEngines";

/**
 * REQ-1512: on a hosted deployment the engine is REPORTED, not chosen.
 *
 * The lane and the size come from the plan (REQ-1510), so there is no lane selector, no engine-kind
 * list and no external endpoint form here — absent rather than shown disabled, because an
 * organization-operated engine is an enterprise arrangement sold on no hosted plan and is not
 * something a hosted administrator is invited to consider and then refused. Billing is named
 * instead, that being where the plan which decides all of this is changed.
 */
export function HostedEngineReport({
  state,
  billingLink = true,
}: {
  state: OrgEngineState;
  billingLink?: boolean;
}) {
  const { t } = useTranslation();
  return (
    <Stack gap="md" data-testid="org-engine-hosted">
      <Text c="dimmed" size="sm">
        {t("orgEngineTab.hostedIntro")}
      </Text>
      <LaneFacts state={state} />
      {billingLink && (
        <Alert color="blue" variant="light" data-testid="org-engine-hosted-change">
          <Link to="/admin/billing">{t("orgEngineTab.hostedChange")}</Link>
        </Alert>
      )}
    </Stack>
  );
}

function LaneFacts({ state, children }: { state: OrgEngineState; children?: ReactNode }) {
  const { t } = useTranslation();
  const laneKey =
    state.mode === "isolated"
      ? "orgEngineTab.hostedLaneIsolated"
      : state.mode === "external"
        ? "orgEngineTab.hostedLaneExternal"
        : "orgEngineTab.hostedLaneShared";
  const engineState = state.isolated_engine?.state ?? null;
  const stateText =
    engineState === null
      ? null
      : engineState === "ready" || engineState === "running"
        ? t("orgEngineTab.hostedStateReady")
        : engineState === "starting"
          ? t("orgEngineTab.hostedStateStarting")
          : engineState === "stopped" || engineState === "exited"
            ? t("orgEngineTab.hostedStateStopped")
            : engineState === "absent"
              ? t("orgEngineTab.hostedStateAbsent")
              : t("orgEngineTab.hostedStateOther", { state: engineState });

  return (
    <Stack gap="xs">
      <Text fw={600}>{t("orgEngineTab.hostedLane")}</Text>
      <Text size="sm" data-testid="org-engine-hosted-lane">
        {t(laneKey)}
      </Text>
      {state.plan && (
        <Text size="sm" data-testid="org-engine-hosted-plan">
          {t("orgEngineTab.hostedPlan", { plan: state.plan })}
        </Text>
      )}
      {state.engine_size && (
        <Text size="sm" data-testid="org-engine-hosted-size">
          {t("orgEngineTab.hostedSize", {
            machine: state.engine_size.machine_type,
            vcpu: state.engine_size.vcpu,
            memory: state.engine_size.memory_gib,
          })}
        </Text>
      )}
      {children}
      {stateText && (
        <Text size="sm" data-testid="org-engine-hosted-state">
          {stateText}
        </Text>
      )}
    </Stack>
  );
}

// REQ-1922: when the org uses regions, its engine in each is the store the platform assigned it
// there, so this page is a tab per region, as Admin > Federation is; with none, the lane as before.
export function OrgEngineTab() {
  const { t } = useTranslation();
  const regions = useRegionEngines();
  if (regions.status === "error") return <Alert color="red">{regions.text}</Alert>;
  if (regions.status === "loading") return <Text>{t("orgEngineTab.loading")}</Text>;
  if (regions.status === "none") return <OrgEngineLane />;
  return <RegionEngineTabs grant={regions.grant} save={regions.save} sandbox={regions.sandbox} />;
}

// REQ-1412: the org's engine lane, read-only under org_settings. Saving it is the platform's, on
// the Platform page (PlatformOrgEngine), naming the org.
function OrgEngineLane() {
  const { t } = useTranslation();
  const [state, setState] = useState<OrgEngineState | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    fetchOrgEngine()
      .then(setState)
      .catch((e) => setError(String(e)));
  }, []);

  if (error) return <Alert color="red">{error}</Alert>;
  if (!state) return <Text>{t("orgEngineTab.loading")}</Text>;
  return state.plan_derived ? (
    <HostedEngineReport state={state} />
  ) : (
    <OrgEngineReport state={state} />
  );
}

/** What the org runs on where no plan decides it: the platform sets the lane, the org reads it. */
function OrgEngineReport({ state }: { state: OrgEngineState }) {
  const { t } = useTranslation();
  const external = state.mode === "external";
  const kind =
    state.external_kinds.find((k) => k.key === state.engine_kind)?.label ?? state.engine_kind;
  return (
    <Stack gap="md" data-testid="org-engine-report">
      <Text c="dimmed" size="sm">
        {t("orgEngineTab.reportIntro", { engine: state.engine_name })}
      </Text>
      <LaneFacts state={state}>
        {external && kind && (
          <Text size="sm" data-testid="org-engine-report-kind">
            {t("orgEngineTab.reportKind", { kind })}
          </Text>
        )}
        {external && state.external_host && (
          <Text size="sm" data-testid="org-engine-report-endpoint">
            {t("orgEngineTab.reportEndpoint", {
              host: state.external_host,
              port: state.external_port,
            })}
          </Text>
        )}
        {external && state.external_url_set && (
          <Text size="sm" data-testid="org-engine-report-url">
            {t("orgEngineTab.reportUrlStored")}
          </Text>
        )}
      </LaneFacts>
      <Alert color="blue" variant="light" data-testid="org-engine-report-change">
        {t("orgEngineTab.reportChange")}
      </Alert>
    </Stack>
  );
}
