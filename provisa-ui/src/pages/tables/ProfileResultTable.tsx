// Copyright (c) 2026 Kenneth Stott
// Canary: 403ccf4d-f2dd-4751-bcb9-f0bc30113632
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123 (amended 2026-10-21): every table of the profile runs modal is the explorer's results
// grid (ResultsGrid): it sorts, groups and filters by column, and downloads as xlsx; a table of a
// run's results has an expand button that draws it again in the 95% modal, as the 3D views do
// (Expandable3D).

import { Group } from "@mantine/core";
import { ResultsGrid } from "../sql/ResultsGrid";
import { useResultsGrid } from "../sql/useResultsGrid";
import { ExpandButton, Expandable3D } from "./profile3d/Expandable3D";
import "./profile3d/profile3d.css";

type Row = Record<string, unknown>;

interface Props {
  /** The rows as shown: one plain value per column. */
  rows: Row[];
  columns: string[];
  testId: string;
  /** The expanded copy's title, and the downloaded workbook's name. */
  title: string;
  /** The in-place copy's height; the expanded one fills its modal. */
  height: number;
  expandable?: boolean;
  onRowClick?: (row: Row) => void;
  selectedRow?: (row: Row) => boolean;
}

function Grid({ rows, columns, title, onRowClick, selectedRow }: Props) {
  const grid = useResultsGrid(rows, columns);
  return (
    <ResultsGrid
      grid={grid}
      totalRowCount={rows.length}
      exportName={title}
      onRowClick={onRowClick}
      selectedRow={selectedRow}
    />
  );
}

export function ProfileResultTable(props: Props) {
  const { testId, height, expandable = true, title } = props;
  if (!expandable) {
    return (
      <div style={{ height }} data-testid={testId}>
        <Grid {...props} />
      </div>
    );
  }
  return (
    <Expandable3D
      title={title}
      testId={testId}
      render={(expanded, onExpand) =>
        expanded ? (
          <div style={{ height: "100%" }} data-testid={`${testId}-expanded`}>
            <Grid {...props} />
          </div>
        ) : (
          <>
            <Group justify="flex-end" mb={4}>
              {onExpand && <ExpandButton onClick={onExpand} testId={`${testId}-expand`} />}
            </Group>
            <div style={{ height }} data-testid={testId}>
              <Grid {...props} />
            </div>
          </>
        )
      }
    />
  );
}
