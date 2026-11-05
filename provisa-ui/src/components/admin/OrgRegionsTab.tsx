// Copyright (c) 2026 Kenneth Stott
// Canary: 3c9e1f74-5a28-4d06-b7e3-91f0a6c2d8b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: Admin > Regions -- the regions the platform granted the org and the store it keeps
// each kind of data in there, for the org administrator to read. Which regions an org uses is the
// platform's decision, made on the Platform page; within them the org decides where its tables and
// sources live, the residency grants on its roles, and the default region for new objects.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Stack, Text } from "@mantine/core";
import { TriangleAlert } from "lucide-react";
import { fetchOrgRegions, problemOf, type GrantView } from "../../api/regions";
import { PageLoading } from "../PageLoading";
import { GrantEditor } from "./GrantEditor";

export function OrgRegionsTab() {
  const { t } = useTranslation();
  const [data, setData] = useState<GrantView | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    fetchOrgRegions()
      .then(setData)
      .catch((e: unknown) => setError(problemOf(e).text));
  }, []);

  if (error) {
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="org-regions-error">
        {error}
      </Alert>
    );
  }
  if (data === null) return <PageLoading message={t("orgRegions.loading")} />;
  if (data.platform_regions.length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid="org-regions-none-declared">
        {t("orgRegions.noPlatformRegions")}
      </Text>
    );
  }
  return (
    <Stack gap="sm">
      <Text c="dimmed" size="sm" maw={820} data-testid="org-regions-granted-note">
        {t("orgRegions.grantedIntro")}
      </Text>
      <GrantEditor data={data} storageKey="provisa.orgRegions.detailWidth" />
    </Stack>
  );
}
