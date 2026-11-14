// Copyright (c) 2026 Kenneth Stott
// Canary: 5a3f8d12-7c46-4e9b-b1d0-2e8c6f4a9b73
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (amended 2026-11-11, THE PRODUCT PAGE IS SUMMARY ROWS): an offer's terms of use --
// purpose, usage and limitations, in prose -- as its card and its contracts show them, each term
// it states on a line of its own.

import { useTranslation } from "react-i18next";
import { Stack, Text } from "@mantine/core";
import { TERMS_OF_USE, type TermOfUse } from "../../../api/dataProductOffers";

export function TermsOfUse({
  terms,
  testId,
}: {
  terms: Record<TermOfUse, string>;
  testId: string;
}) {
  const { t } = useTranslation();
  const stated = TERMS_OF_USE.filter((term) => terms[term].trim() !== "");
  if (stated.length === 0) {
    return (
      <Text size="xs" c="dimmed" data-testid={`${testId}-none`}>
        {t("dataProductOffers.noTermsOfUse")}
      </Text>
    );
  }
  return (
    <Stack gap={2} data-testid={testId}>
      {stated.map((term) => (
        <Text key={term} size="xs" data-testid={`${testId}-${term}`}>
          <Text span size="xs" fw={600}>
            {t(`dataProductOffers.term_${term}`)}:
          </Text>{" "}
          {terms[term]}
        </Text>
      ))}
    </Stack>
  );
}
