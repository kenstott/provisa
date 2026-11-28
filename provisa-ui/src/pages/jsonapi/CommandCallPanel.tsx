// Copyright (c) 2026 Kenneth Stott
// Canary: 6a0f2d84-1c37-4b95-9e6d-7f3a81c5d2b0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-27): the JSON:API Explorer showing one command's call: its arguments
// to fill, Run, the request, and the rows it answered (useCommandCall).

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Anchor, Badge, Button, Table, Tabs, Text, TextInput, Title } from "@mantine/core";
import { AlertCircle } from "lucide-react";
import { commandRows, type useCommandCall } from "./useCommandCall";

type CommandCall = ReturnType<typeof useCommandCall>;

export function CommandCallPanel({ state }: { state: CommandCall }) {
  const { t } = useTranslation();
  const [viewTab, setViewTab] = useState<"summary" | "raw">("summary");
  const { call, fields, setField, url, doc, running, error, run, close } = state;
  if (call === null) return null;
  const rows = doc ? commandRows(doc) : [];
  const columns = [...new Set(rows.flatMap((r) => Object.keys(r)))];
  return (
    <div className="jsonapi-page page" data-testid="jsonapi-command-call">
      <div className="jsonapi-layout">
        <div className="jsonapi-sidebar">
          <div className="jsonapi-sidebar-header">
            <Title order={2} className="jsonapi-section-title">
              {t("jsonApiPage.commandTitle", { name: call.name })}
            </Title>
            <Anchor
              component="button"
              type="button"
              className="jsonapi-spec-link"
              onClick={close}
              data-testid="jsonapi-command-close"
            >
              {t("jsonApiPage.commandBack")}
            </Anchor>
          </div>
          <div className="jsonapi-section-divider">{t("jsonApiPage.commandArguments")}</div>
          {fields.length === 0 && (
            <Text size="xs" data-testid="jsonapi-command-no-arguments">
              {t("jsonApiPage.commandNoArguments")}
            </Text>
          )}
          {fields.map((f) => (
            <TextInput
              key={f.name}
              size="xs"
              label={f.name}
              value={f.text}
              onChange={(e) => setField(f.name, e.currentTarget.value)}
              data-testid={`jsonapi-command-arg-${f.name}`}
            />
          ))}
          <Button
            className="jsonapi-run-btn"
            onClick={() => void run()}
            loading={running}
            mt="sm"
            data-testid="jsonapi-run-button"
          >
            {running ? t("jsonApiPage.runningButton") : t("jsonApiPage.runButton")}
          </Button>
        </div>
        <div className="jsonapi-main">
          <div className="jsonapi-url-bar">
            <Badge className="jsonapi-method" color="green" variant="light" radius="sm">
              {call.write ? t("jsonApiPage.methodPost") : t("jsonApiPage.methodGet")}
            </Badge>
            <span className="jsonapi-url" data-testid="jsonapi-command-url">
              {url}
            </span>
          </div>
          <div className="jsonapi-panel">
            <div className="jsonapi-panel-header">
              <div className="jsonapi-panel-header-left">
                <Text size="xs" fw={600} tt="uppercase">
                  {t("jsonApiPage.responseLabel")}
                </Text>
                {doc && (
                  <span className="jsonapi-result-count">
                    {t("jsonApiPage.resultCount", { count: rows.length })}
                  </span>
                )}
              </div>
              {doc && (
                <Tabs
                  value={viewTab}
                  onChange={(v) => setViewTab((v as "summary" | "raw") ?? "summary")}
                  className="jsonapi-tab-bar"
                >
                  <Tabs.List>
                    <Tabs.Tab value="summary" data-testid="jsonapi-summary-tab">
                      {t("jsonApiPage.summaryTab")}
                    </Tabs.Tab>
                    <Tabs.Tab value="raw" data-testid="jsonapi-raw-tab">
                      {t("jsonApiPage.rawTab")}
                    </Tabs.Tab>
                  </Tabs.List>
                </Tabs>
              )}
            </div>
            {error && (
              <Alert
                className="jsonapi-error"
                color="red"
                icon={<AlertCircle size={16} />}
                variant="light"
              >
                {error}
              </Alert>
            )}
            {doc && viewTab === "summary" ? (
              rows.length === 0 ? (
                <div className="jsonapi-summary-empty">{t("jsonApiPage.noResults")}</div>
              ) : (
                <div className="jsonapi-attr-scroll">
                  <Table className="jsonapi-attr-table" data-testid="jsonapi-command-rows">
                    <Table.Thead>
                      <Table.Tr>
                        {columns.map((c) => (
                          <Table.Th key={c}>{c}</Table.Th>
                        ))}
                      </Table.Tr>
                    </Table.Thead>
                    <Table.Tbody>
                      {rows.map((r, i) => (
                        <Table.Tr key={i}>
                          {columns.map((c) => (
                            <Table.Td key={c}>
                              {r[c] === null || r[c] === undefined ? (
                                <span className="jsonapi-attr-null">
                                  {t("jsonApiPage.nullValue")}
                                </span>
                              ) : typeof r[c] === "object" ? (
                                JSON.stringify(r[c])
                              ) : (
                                String(r[c])
                              )}
                            </Table.Td>
                          ))}
                        </Table.Tr>
                      ))}
                    </Table.Tbody>
                  </Table>
                </div>
              )
            ) : (
              <pre className="jsonapi-response-text">
                {doc
                  ? JSON.stringify(doc, null, 2)
                  : running
                    ? t("jsonApiPage.rawPlaceholderRunning")
                    : t("jsonApiPage.rawPlaceholderEmpty")}
              </pre>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
