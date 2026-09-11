// Copyright (c) 2026 Kenneth Stott
// Canary: 7c1e4a52-8d3b-42f7-9c60-1b8ef4a2d905
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1408/REQ-1722: the checkbox tree for the JSON:API Explorer's ?include= picker, split out
// of JsonApiPage.tsx to keep that file under the max-lines gate. The group-by nodes projection the
// backend resolves (provisa/api/jsonapi/generator.py::_insert_jsonapi_include_path) accepts a
// dot-path at any depth, but the picker offers only two levels of relationship — a table's direct
// relationships plus theirs — past which the checkbox tree would grow without bound for little
// practical use; a path the picker cannot reach is still typeable by hand.

import { Checkbox } from "@mantine/core";

export interface IncludeSubRel {
  name: string;
  columns: string[];
}

export interface IncludeRel extends IncludeSubRel {
  targetTableId: unknown;
  subRels: IncludeSubRel[];
}

interface IncludeTreeProps {
  relationships: IncludeRel[];
  checkedIncludes: Set<string>;
  showColumns: boolean;
  toggleInclude: (path: string) => void;
  toggleIncludeColumn: (path: string, column: string) => void;
}

export function IncludeTree({
  relationships,
  checkedIncludes,
  showColumns,
  toggleInclude,
  toggleIncludeColumn,
}: IncludeTreeProps) {
  return (
    <div className="jsonapi-field-list">
      {relationships.map((rel) => (
        <div key={rel.name}>
          <Checkbox
            className="jsonapi-field-item"
            size="xs"
            checked={checkedIncludes.has(rel.name)}
            onChange={() => toggleInclude(rel.name)}
            label={<span className="jsonapi-field-name">{rel.name}</span>}
          />
          {/* Per-column dot-paths, and one further level of relationship, only apply to the
              group-by nodes projection — a plain include sideloads whole related resources. */}
          {showColumns && (
            <>
              {rel.columns.map((col) => (
                <Checkbox
                  key={`${rel.name}.${col}`}
                  className="jsonapi-field-item jsonapi-field-item-nested"
                  size="xs"
                  checked={checkedIncludes.has(`${rel.name}.${col}`)}
                  onChange={() => toggleIncludeColumn(rel.name, col)}
                  data-testid={`jsonapi-include-${rel.name}.${col}`}
                  label={
                    <span className="jsonapi-field-name">
                      {rel.name}.{col}
                    </span>
                  }
                />
              ))}
              {rel.subRels.map((subRel) => {
                const subPath = `${rel.name}.${subRel.name}`;
                return (
                  <div key={subPath}>
                    <Checkbox
                      className="jsonapi-field-item jsonapi-field-item-nested"
                      size="xs"
                      checked={checkedIncludes.has(subPath)}
                      onChange={() => toggleInclude(subPath)}
                      data-testid={`jsonapi-include-${subPath}`}
                      label={<span className="jsonapi-field-name">{subPath}</span>}
                    />
                    {subRel.columns.map((col) => (
                      <Checkbox
                        key={`${subPath}.${col}`}
                        className="jsonapi-field-item jsonapi-field-item-nested-2"
                        size="xs"
                        checked={checkedIncludes.has(`${subPath}.${col}`)}
                        onChange={() => toggleIncludeColumn(subPath, col)}
                        data-testid={`jsonapi-include-${subPath}.${col}`}
                        label={
                          <span className="jsonapi-field-name">
                            {subPath}.{col}
                          </span>
                        }
                      />
                    ))}
                  </div>
                );
              })}
            </>
          )}
        </div>
      ))}
    </div>
  );
}
