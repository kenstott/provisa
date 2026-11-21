// Copyright (c) 2026 Kenneth Stott
// Canary: f52fe2c4-38f2-495e-95a2-1ef422aea7b7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Stack, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import { useVectorModels, vectorOf } from "./useVectorModels";
import { VectorModelCell } from "./VectorModelCell";

/** One column as the vector editor reads it. */
interface VectorColumn {
  name: string;
  path?: string | null;
  embeddingModel?: string | null;
}

/** REQ-2000: in the Columns section of a Qdrant collection's table (unsaved, opened through
 * Register, or saved), each vector column names the registered model it is an embedding of,
 * chosen from the models of the vector's size. */
export function QdrantVectorColumns({
  sourceId,
  collection,
  columns,
  onModel,
}: {
  sourceId: string;
  collection: string;
  columns: VectorColumn[];
  onModel: (index: number, model: string) => void;
}) {
  const { t } = useTranslation();
  const models = useVectorModels(sourceId, collection, true);
  const vectors = columns.flatMap((c, i) => (vectorOf(c.path) === null ? [] : [{ c, i }]));
  if (vectors.length === 0) return null;
  return (
    <FormField size="full">
      <Stack gap="xs" data-testid="register-table-vector-models">
        <Text size="sm">{t("vectorModelCell.header")}</Text>
        {vectors.map(({ c, i }) => (
          <Stack key={c.name} gap={2} maw="20rem">
            <Text size="xs" ff="monospace">
              {c.name}
            </Text>
            <VectorModelCell
              column={c.name}
              path={c.path}
              value={c.embeddingModel}
              models={models}
              onChange={(m) => onModel(i, m ?? "")}
            />
          </Stack>
        ))}
      </Stack>
    </FormField>
  );
}
