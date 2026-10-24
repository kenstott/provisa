// Copyright (c) 2026 Kenneth Stott
// Canary: 74cae7d7-d840-4bdf-b6fa-7c60a709198c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1660 (ODPS inputPorts): a dataset the product reads, one row of its Input Ports card.

import { useId, useState } from "react";
import { Group, List, Text, UnstyledButton } from "@mantine/core";
import { ChevronDown, ChevronRight } from "lucide-react";

// ODPS inputPorts: a dataset one lineage hop upstream of this product's member tables, derived (not
// stored) by the parent page's inputPortsFor() from the lineage graph.
export interface InputPortRow {
  id: string; // the dataset's relation: a key, never shown
  table: string; // the table as the catalog names it
  columns: string[]; // the columns of it the product reads
}

/** One input port: the table, collapsed; open, the columns of it the product reads. */
export function InputPort({ row }: { row: InputPortRow }) {
  const [open, setOpen] = useState(false);
  const bodyId = useId();
  return (
    <div data-testid={`input-port-${row.id}`}>
      <UnstyledButton
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        aria-controls={bodyId}
        data-testid={`input-port-toggle-${row.id}`}
      >
        <Group gap={4} wrap="nowrap">
          {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          <Text size="sm" ff="monospace">
            {row.table}
          </Text>
        </Group>
      </UnstyledButton>
      {open && (
        <List
          size="xs"
          id={bodyId}
          pl={22}
          ff="monospace"
          data-testid={`input-port-columns-${row.id}`}
        >
          {row.columns.map((c) => (
            <List.Item key={c}>{c}</List.Item>
          ))}
        </List>
      )}
    </div>
  );
}
