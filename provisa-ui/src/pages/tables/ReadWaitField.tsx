// Copyright (c) 2026 Kenneth Stott
// Canary: 8e4d2b71-3c95-4f0a-b6d2-5a17c9e3f084
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { NumberInput, Select } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";

/** The stored value that makes a read with nothing landed answer no rows. */
export const READ_WAIT_NO_ROWS = 0;
const DEFAULT_WAIT_SECONDS = 300;

type Mode = "request" | "wait" | "noRows";

function modeOf(readWait: number | null): Mode {
  if (readWait === null) return "request";
  return readWait === READ_WAIT_NO_ROWS ? "noRows" : "wait";
}

/** What a read does while nothing of the table has landed. */
export function ReadWaitField({
  value,
  onChange,
}: {
  value: number | null;
  onChange: (readWait: number | null) => void;
}) {
  const { t } = useTranslation();
  const mode = modeOf(value);
  const choose = (next: string | null) => {
    if (next === "request") onChange(null);
    else if (next === "noRows") onChange(READ_WAIT_NO_ROWS);
    else if (next === "wait" && mode !== "wait") onChange(DEFAULT_WAIT_SECONDS);
  };
  return (
    <>
      <FormField size="medium" help={t("landingScope.readWaitHelp")}>
        <Select
          label={t("landingScope.readWaitLabel")}
          data={[
            { value: "request", label: t("landingScope.readWaitRequest") },
            { value: "wait", label: t("landingScope.readWaitTimed") },
            { value: "noRows", label: t("landingScope.readWaitNoRows") },
          ]}
          value={mode}
          onChange={choose}
          allowDeselect={false}
          data-testid="table-read-wait-select"
        />
      </FormField>
      {mode === "wait" && (
        <FormField size="short">
          <NumberInput
            min={1}
            allowDecimal={false}
            label={t("landingScope.readWaitSeconds")}
            value={value ?? DEFAULT_WAIT_SECONDS}
            onChange={(v) => {
              if (typeof v === "number" && v >= 1) onChange(v);
            }}
            data-testid="table-read-wait-seconds"
          />
        </FormField>
      )}
    </>
  );
}
