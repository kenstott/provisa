// Copyright (c) 2026 Kenneth Stott
// Canary: 88e7f591-36d7-47a0-9b56-5d4e0e2f6244
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Group, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { ContextPlace } from "../api/contextViews";
import { EntityLink } from "./EntityLink";
import { placeName } from "./contextPlaceTexts";

/** REQ-2259: the tables and columns a tag is applied to or a kind was detected on, each a link to
 * its panel when the viewer holds the table page's right; those the viewer does not read are only
 * counted. */
export function ContextPlaces({
  places,
  hidden,
  testId,
}: {
  places: ContextPlace[];
  hidden: number;
  testId: string;
}) {
  const { t } = useTranslation();
  if (places.length === 0 && hidden === 0) {
    return (
      <Text size="xs" c="dimmed" data-testid={testId}>
        {t("contextViews.nowhere")}
      </Text>
    );
  }
  return (
    <Group gap={6} data-testid={testId}>
      {places.map((p) => {
        const name = placeName(p);
        return (
          <Text size="xs" key={name} component="span">
            {p.column === null ? (
              <EntityLink kind="table" id={p.tableId} testId={`${testId}-place`}>
                {name}
              </EntityLink>
            ) : (
              <EntityLink kind="column" id={p.column} table={p.tableId} testId={`${testId}-place`}>
                {name}
              </EntityLink>
            )}
          </Text>
        );
      })}
      {hidden > 0 && (
        <Text size="xs" c="dimmed" component="span" data-testid={`${testId}-hidden`}>
          {t("contextViews.hiddenPlaces", { count: hidden })}
        </Text>
      )}
    </Group>
  );
}
