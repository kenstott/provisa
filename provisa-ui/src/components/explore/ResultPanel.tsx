// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4e1a77-2f6b-4c3e-8a05-6b1f3e7c9d24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the one result panel of every Explore surface that shows results, after Cypher's: a
// strip at its top (what it is, its views -- table, graph, overview, raw -- and its actions), then
// the result itself edge to edge below a single divider. No card, no padding around the result:
// a grid, a graph or a document fills the panel and scrolls inside it.

import type { ReactNode } from "react";
import { Badge, Tabs, Text } from "@mantine/core";

export interface ResultView {
  value: string;
  label: ReactNode;
  /** A count shown beside the label (rows, errors); none when 0 or absent. */
  count?: number;
  /** The count's color: red for errors. */
  countColor?: string;
  icon?: ReactNode;
  testId?: string;
}

export function ResultPanel({
  testId,
  title,
  meta,
  views,
  view,
  onViewChange,
  actions,
  notice,
  children,
  answered,
}: {
  testId: string;
  /** What the panel holds, at the start of its strip ("Response", "Results"). */
  title?: ReactNode;
  /** Beside the title: a count, a time. */
  meta?: ReactNode;
  /** The panel's views, switched in its strip. */
  views?: ResultView[];
  view?: string;
  onViewChange?: (view: string) => void;
  /** At the end of its strip. */
  actions?: ReactNode;
  /** Between the strip and the result: an error, a pager, a statement picker. */
  notice?: ReactNode;
  children: ReactNode;
  /** REQ-2010: marked once a result is in, which a demo tour step waits for. */
  answered?: boolean;
}) {
  const head = title !== undefined || meta !== undefined || views !== undefined || actions;
  return (
    <section
      className="result-panel"
      data-result-panel=""
      data-testid={testId}
      data-answered={answered ? "" : undefined}
    >
      {head && (
        <div className="result-panel__strip">
          {title !== undefined && (
            <Text size="xs" fw={600} tt="uppercase" className="result-panel__title">
              {title}
            </Text>
          )}
          {meta !== undefined && <span className="result-panel__meta">{meta}</span>}
          {views && (
            <Tabs
              value={view ?? null}
              onChange={(v) => v !== null && onViewChange?.(v)}
              keepMounted={false}
              className="result-panel__views"
            >
              <Tabs.List>
                {views.map((v) => (
                  <Tabs.Tab
                    key={v.value}
                    value={v.value}
                    data-testid={v.testId}
                    leftSection={v.icon}
                    rightSection={
                      v.count !== undefined && v.count > 0 ? (
                        <Badge size="xs" circle color={v.countColor ?? "blue"}>
                          {v.count}
                        </Badge>
                      ) : undefined
                    }
                  >
                    {v.label}
                  </Tabs.Tab>
                ))}
              </Tabs.List>
            </Tabs>
          )}
          {actions && <div className="result-panel__actions">{actions}</div>}
        </div>
      )}
      {notice}
      <div className="result-panel__body">{children}</div>
    </section>
  );
}
