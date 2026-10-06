// Copyright (c) 2026 Kenneth Stott
// Canary: 5b9a3e07-2d61-4c84-8f1b-c6e0a9d27f15
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Checkbox, Group, NumberInput, Select, Stack, TagsInput, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { EMPTY_SEED, type SyntheticSeed } from "./syntheticSeed";

// REQ-1939, BOOTSTRAPPING AN ENVIRONMENT: creating an environment may seed it with a synthetic
// dataset -- the tables, the environment whose profile runs describe them and a scale -- so it
// starts on plausible data and no production row reaches it.
export function SyntheticSeedFields({
  envs,
  seed,
  setSeed,
}: {
  envs: string[];
  seed: SyntheticSeed | null;
  setSeed: (seed: SyntheticSeed | null) => void;
}) {
  const { t } = useTranslation();
  return (
    <Stack gap={4}>
      <Checkbox
        label={t("syntheticDatasets.seedOption")}
        checked={seed !== null}
        onChange={(e) => setSeed(e.currentTarget.checked ? EMPTY_SEED : null)}
        data-testid="env-new-synthetic"
      />
      {seed !== null && (
        <Group align="end">
          <TextInput
            label={t("syntheticDatasets.name")}
            value={seed.dataset}
            onChange={(e) => setSeed({ ...seed, dataset: e.currentTarget.value })}
            data-testid="env-new-synthetic-name"
          />
          <TagsInput
            label={t("syntheticDatasets.seedTables")}
            value={seed.tables}
            onChange={(tables) => setSeed({ ...seed, tables })}
            data-testid="env-new-synthetic-tables"
          />
          <Select
            label={t("syntheticDatasets.profileEnv")}
            data={envs}
            value={seed.profile_env}
            onChange={(v) => v && setSeed({ ...seed, profile_env: v })}
            allowDeselect={false}
            data-testid="env-new-synthetic-profile-env"
          />
          <NumberInput
            label={t("syntheticDatasets.scale")}
            value={seed.scale}
            min={0.001}
            decimalScale={3}
            onChange={(v) => setSeed({ ...seed, scale: Number(v) })}
            data-testid="env-new-synthetic-scale"
          />
          <NumberInput
            label={t("syntheticDatasets.seed")}
            value={seed.seed}
            allowDecimal={false}
            onChange={(v) => setSeed({ ...seed, seed: Number(v) })}
            data-testid="env-new-synthetic-seed"
          />
        </Group>
      )}
    </Stack>
  );
}
