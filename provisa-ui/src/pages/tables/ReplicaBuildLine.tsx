// Copyright (c) 2026 Kenneth Stott
// Canary: 84888d62-7d68-4bb8-a6db-9453f977a513
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1915: one line under the Replicate drop-down saying where the table's replica build
// stands: requested, running (rows copied, rate, how it copies), built, failed (why) or being
// removed. A failure to bring replicas in line with the model at all is shown beside it.

import { useTranslation } from "react-i18next";
import { Text } from "@mantine/core";
import { useReplicaBuilds } from "../../hooks/useAdminOpsQueries";
import { serverMessage } from "../../i18n/serverMessage";
import { replicaBuildLine } from "./replicaBuild";

export function ReplicaBuildLine({
  sourceId,
  schemaName,
  tableName,
}: {
  sourceId: string;
  schemaName: string;
  tableName: string;
}) {
  const { t, i18n } = useTranslation();
  const { replicaBuilds } = useReplicaBuilds();
  if (replicaBuilds === null) return null;
  const build = replicaBuilds.builds.find(
    (b) => b.sourceId === sourceId && b.schemaName === schemaName && b.tableName === tableName,
  );
  const numbers = new Intl.NumberFormat(i18n.language);
  const line = replicaBuildLine(
    build,
    t,
    (n) => numbers.format(n),
    (iso) => new Date(iso).toLocaleString(i18n.language),
    serverMessage,
  );
  return (
    <>
      <Text size="xs" c={build?.state === "failed" ? "red" : "dimmed"} data-testid="replica-build-line">
        {line}
      </Text>
      {replicaBuilds.convergenceError && (
        <Text size="xs" c="red" data-testid="replica-convergence-error">
          {t("replicaBuild.convergenceError", { error: replicaBuilds.convergenceError })}
        </Text>
      )}
    </>
  );
}
