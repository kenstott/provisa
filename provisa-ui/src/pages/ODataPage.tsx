// Copyright (c) 2026 Kenneth Stott
// Canary: 8b7bec13-df2c-44bd-93ed-cc23639c19e2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-2193

import { useTranslation } from "react-i18next";
import { Code, Group, Stack, Text } from "@mantine/core";
import { Sheet } from "lucide-react";
import { CopyButton } from "../components/CopyButton";
import { PageTitle } from "../components/PageTitle";

const SERVICE_ROOT = "/data/odata";

interface AddressProps {
  testId: string;
  label: string;
  help: string;
  url: string;
}

function Address({ testId, label, help, url }: AddressProps) {
  const { t } = useTranslation();
  return (
    <Stack gap={4}>
      <Text size="sm" fw={600}>
        {label}
      </Text>
      <Group gap="xs" wrap="nowrap">
        <Code data-testid={testId}>{url}</Code>
        <CopyButton text={url} hint={t("odataPage.copyUrl")} />
      </Group>
      <Text size="sm" c="dimmed">
        {help}
      </Text>
    </Stack>
  );
}

/** Where an OData client reaches this deployment. The clients are Excel and Power BI, not this
 * page, so it runs no requests: it gives the addresses to paste. */
export function ODataPage() {
  const { t } = useTranslation();
  const root = `${window.location.origin}${SERVICE_ROOT}`;
  return (
    <div className="page">
      <div className="page-header">
        <PageTitle icon={Sheet}>{t("odataPage.title")}</PageTitle>
      </div>
      <Stack gap="lg" maw={720}>
        <Text>{t("odataPage.description")}</Text>
        <Address
          testId="odata-service-root"
          label={t("odataPage.serviceRoot")}
          help={t("odataPage.serviceRootHelp")}
          url={root}
        />
        <Address
          testId="odata-metadata"
          label={t("odataPage.metadata")}
          help={t("odataPage.metadataHelp")}
          url={`${root}/$metadata`}
        />
      </Stack>
    </div>
  );
}
