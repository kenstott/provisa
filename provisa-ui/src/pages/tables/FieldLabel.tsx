// Copyright (c) 2026 Kenneth Stott
// Canary: 9ccc28d4-d586-4912-b590-fff8123c2b38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { Group, Text, Tooltip } from "@mantine/core";

// A form-field label with an inline ⓘ help tooltip, shared across the table-edit form and its
// extracted panels. ``required`` renders the asterisk inline (so it stays on the label row rather
// than the input's own, which would sit apart from a custom label node).
export function FieldLabel({
  text,
  help,
  required = false,
}: {
  text: string;
  help: string;
  required?: boolean;
}) {
  return (
    <Group gap={4} wrap="nowrap">
      <Text component="span" size="sm">
        {text}
        {required && (
          <Text component="span" c="red" aria-hidden style={{ marginLeft: 2 }}>
            *
          </Text>
        )}
      </Text>
      <Tooltip label={help} multiline w={320}>
        <Text component="span" size="xs" c="dimmed" style={{ cursor: "help", lineHeight: 1 }}>
          ⓘ
        </Text>
      </Tooltip>
    </Group>
  );
}
