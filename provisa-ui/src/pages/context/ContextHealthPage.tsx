// Copyright (c) 2026 Kenneth Stott
// Canary: 9d3a6f18-2e7c-4b05-a1d9-c84f0e6b7253
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { HeartPulse } from "lucide-react";
import { useTranslation } from "react-i18next";
import { SimpleGrid, Stack } from "@mantine/core";
import { PageTitle } from "../../components/PageTitle";
import { StewardshipSummary } from "../../components/admin/StewardshipSummary";
import { useCapability } from "../../hooks/useCapability";
import {
  ClassificationCard,
  DocumentsCard,
  DriftCard,
  SyncCard,
  TermsCard,
} from "./ContextHealthCards";

/** REQ-2259: Context Health, the first page of the Context group -- the steward summary with its
 * bulk approval as the page's one action, and a card per health signal already specified. The
 * Context Sync card is drawn only for a holder of that page's right, which every read behind it
 * asks. Metrics has no card until a health signal is defined. */
export function ContextHealthPage() {
  const { t } = useTranslation();
  const canApprove = useCapability("glossary_rw");
  const readsSync = useCapability("org_settings");
  // A bulk approval changes the term counts: the Terms card reads them again.
  const [approvals, setApprovals] = useState(0);
  return (
    <div className="page" data-testid="context-health">
      <div className="page-header">
        <PageTitle icon={HeartPulse}>{t("navBar.itemContextHealth")}</PageTitle>
      </div>
      <Stack gap="lg">
        <StewardshipSummary canEdit={canApprove} onApproved={() => setApprovals((n) => n + 1)} />
        <SimpleGrid cols={{ base: 1, sm: 2, xl: 3 }} spacing="md">
          <TermsCard seq={approvals} />
          <DriftCard />
          <ClassificationCard />
          <DocumentsCard />
          {readsSync && <SyncCard />}
        </SimpleGrid>
      </Stack>
    </div>
  );
}
