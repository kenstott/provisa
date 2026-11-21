// Copyright (c) 2026 Kenneth Stott
// Canary: 96627e23-9cac-4900-b017-3da1bd61c697
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1412, REQ-1327: an operator setting a named org's engine lane -- shared, isolated
// (Provisa-operated) or external (org-operated) -- on the Platform page. The org is picked here and
// named on the route, because the platform role is stripped in a tenant org (REQ-1297), so its
// administrator is never acting in the org it sets the lane for. An org's own administrator changes
// its lane on OrgEngineTab, through the same form. Distinct from FederationEngineTab, which picks
// the engine KIND for the whole deployment.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Select, Stack, Text } from "@mantine/core";
import { TriangleAlert } from "lucide-react";
import {
  fetchPlatformOrgEngine,
  setPlatformOrgEngine,
  type OrgEngineSave,
} from "../../api/admin";
import { fetchOrgGrants, problemOf, type OrgGrantSummary } from "../../api/regions";
import { FormField, FormLayout } from "../form/FormLayout";
import { PageLoading } from "../PageLoading";
import { OrgEngineForm } from "./OrgEngineTab";

/** The Platform page's engine section: pick the org, then set the lane it runs on. */
export function PlatformOrgEngines() {
  const { t } = useTranslation();
  const [orgs, setOrgs] = useState<OrgGrantSummary[] | null>(null);
  const [orgId, setOrgId] = useState<string | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    fetchOrgGrants()
      .then(setOrgs)
      .catch((e: unknown) => setError(problemOf(e).text));
  }, []);

  if (error) {
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="platform-engines-error">
        {error}
      </Alert>
    );
  }
  if (orgs === null) return <PageLoading message={t("platformRegions.loadingOrgs")} />;
  return (
    <Stack gap="sm" data-testid="platform-engines">
      <Text c="dimmed" size="sm" maw={820}>
        {t("orgEngineTab.platformIntro")}
      </Text>
      <FormLayout data-testid="platform-engine-org-form">
        <FormField size="medium">
          <Select
            label={t("platformRegions.org")}
            placeholder={t("platformRegions.pickOrg")}
            data={orgs.map((o) => o.id)}
            value={orgId}
            onChange={setOrgId}
            searchable
            comboboxProps={{ withinPortal: true }}
            data-testid="platform-engine-org-pick"
          />
        </FormField>
      </FormLayout>
      {orgId !== null && <PlatformOrgEngine key={orgId} orgId={orgId} />}
    </Stack>
  );
}

export function PlatformOrgEngine({ orgId }: { orgId: string }) {
  const load = useCallback(() => fetchPlatformOrgEngine(orgId), [orgId]);
  const save = useCallback((body: OrgEngineSave) => setPlatformOrgEngine(orgId, body), [orgId]);
  // No Billing link: that page is the acting org's, not the one picked here.
  return <OrgEngineForm load={load} save={save} billingLink={false} />;
}
