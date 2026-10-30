// Copyright (c) 2026 Kenneth Stott
// Canary: 7c1e94d2-5a36-4b08-9f1d-3e8b6a20c4d7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: above the SPARQL query bar, what an RDF client needs: the endpoint's URL for other
// tools, and the prefixes in use, shown and editable. They name terms for showing and what a pill's
// query declares; a query runs exactly as written.

import { useState } from "react";
import { ActionIcon, Button, Text, TextInput } from "@mantine/core";
import { ChevronDown, ChevronRight, Plus, X } from "lucide-react";
import { useTranslation } from "react-i18next";
import { CopySymbolButton } from "../CopyButton";
import { Tip } from "../Tip";
import type { PrefixEdits } from "./sparql";

const PREFIX_NAME = /^[A-Za-z_][\w-]*$/;

function PrefixRow({
  prefix,
  namespace,
  onSave,
  onRemove,
}: {
  prefix: string;
  namespace: string;
  /** The row's prefix and namespace, as changed. */
  onSave: (prefix: string, namespace: string) => void;
  onRemove?: () => void;
}) {
  const { t } = useTranslation();
  const [name, setName] = useState(prefix);
  const [ns, setNs] = useState(namespace);
  const nameValid = PREFIX_NAME.test(name);
  const save = () => {
    if (nameValid && ns !== "" && (name !== prefix || ns !== namespace)) onSave(name, ns);
  };
  return (
    <div className="graph-prefix-row" data-testid={`sparql-prefix-row-${prefix}`}>
      <TextInput
        size="xs"
        aria-label={t("sparqlEndpoint.prefixName")}
        placeholder={t("sparqlEndpoint.prefixName")}
        value={name}
        error={name !== "" && !nameValid ? t("sparqlEndpoint.invalidPrefix") : undefined}
        onChange={(e) => setName(e.currentTarget.value)}
        onBlur={save}
        data-testid={`sparql-prefix-name-${prefix}`}
      />
      <TextInput
        size="xs"
        className="graph-prefix-namespace"
        aria-label={t("sparqlEndpoint.namespace")}
        placeholder={t("sparqlEndpoint.namespace")}
        value={ns}
        onChange={(e) => setNs(e.currentTarget.value)}
        onBlur={save}
        data-testid={`sparql-prefix-namespace-${prefix}`}
      />
      {onRemove && (
        <Tip text={t("sparqlEndpoint.removePrefix")} named>
          <ActionIcon
            variant="subtle"
            size="sm"
            aria-label={t("sparqlEndpoint.removePrefix")}
            onClick={onRemove}
            data-testid={`sparql-prefix-remove-${prefix}`}
          >
            <X size={14} aria-hidden />
          </ActionIcon>
        </Tip>
      )}
    </div>
  );
}

export function SparqlEndpointBar({
  base,
  prefixes,
  edits,
  onEdit,
}: {
  /** The address the endpoint is published under (server.public_url). */
  base: string;
  /** The prefixes in use. */
  prefixes: Record<string, string>;
  edits: PrefixEdits;
  onEdit: (edits: PrefixEdits) => void;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [adding, setAdding] = useState(0);
  const endpoint = `${base}/data/sparql`;
  const rows = Object.entries(prefixes).sort(([a], [b]) => a.localeCompare(b));
  return (
    <div className="graph-endpoint-bar" data-testid="sparql-endpoint-bar">
      <div className="graph-endpoint-line">
        <Text component="span" size="xs" c="dimmed">
          {t("sparqlEndpoint.endpoint")}
        </Text>
        <code className="graph-endpoint-url" data-testid="sparql-endpoint-url">
          {endpoint}
        </code>
        <CopySymbolButton
          text={endpoint}
          className="graph-endpoint-copy"
          title={t("sparqlEndpoint.copyEndpoint")}
        />
        <Button
          size="compact-xs"
          variant="subtle"
          leftSection={open ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
          aria-expanded={open}
          onClick={() => setOpen((v) => !v)}
          data-testid="sparql-prefixes-toggle"
        >
          {t("sparqlEndpoint.prefixes", { count: rows.length })}
        </Button>
      </div>
      {open && (
        <div className="graph-prefix-list" data-testid="sparql-prefix-list">
          {rows.map(([prefix, ns]) => (
            <PrefixRow
              key={`${prefix} ${ns}`}
              prefix={prefix}
              namespace={ns}
              onSave={(name, namespace) =>
                onEdit({
                  ...edits,
                  ...(name === prefix ? {} : { [prefix]: null }),
                  [name]: namespace,
                })
              }
              onRemove={() => onEdit({ ...edits, [prefix]: null })}
            />
          ))}
          <PrefixRow
            key={`new ${adding}`}
            prefix=""
            namespace=""
            onSave={(name, namespace) => {
              onEdit({ ...edits, [name]: namespace });
              setAdding((n) => n + 1);
            }}
          />
          <div className="graph-prefix-actions">
            <Text component="span" size="xs" c="dimmed">
              <Plus size={10} aria-hidden /> {t("sparqlEndpoint.addHint")}
            </Text>
            {Object.keys(edits).length > 0 && (
              <Button
                size="compact-xs"
                variant="default"
                onClick={() => onEdit({})}
                data-testid="sparql-prefixes-restore"
              >
                {t("sparqlEndpoint.restore")}
              </Button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
