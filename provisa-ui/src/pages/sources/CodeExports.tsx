// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: what a saved code source's code exports, as Provisa read it: each function's name and
// kind (table, command, stream). Read only, in the source's read view and its edit form alike.

import { useEffect, useState, type ReactNode } from "react";
import { Input, Text } from "@mantine/core";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { fetchExports, type CodeExport } from "../../api/codeHosts";
import { FormField } from "../../components/form/FormLayout";

type ListProps = { label: string; exports: CodeExport[] | null; failed: string };

function lines(p: ListProps, t: TFunction): ReactNode {
  if (p.failed) return p.failed;
  if (p.exports === null) return t("codeSourceFields.exportsLoading");
  if (!p.exports.length) return t("codeSourceFields.exportsNone");
  return (
    <span data-testid="code-exports-list">
      {p.exports.map((e) => (
        <span key={e.name} style={{ display: "block" }}>
          <code>{e.name}</code> · {t(`codeSourceFields.exportKind_${e.kind}`)}
        </span>
      ))}
    </span>
  );
}

/** The list as a labelled control; read, it is its lines (readValue). */
function ExportList(p: ListProps) {
  const { t } = useTranslation();
  return (
    <Input.Wrapper label={p.label}>
      <Text size="sm" component="div" c={p.failed ? "red" : undefined} data-testid="code-exports">
        {lines(p, t)}
      </Text>
    </Input.Wrapper>
  );
}
ExportList.readValue = (p: Record<string, unknown>, t: TFunction) =>
  lines(p as unknown as ListProps, t);

export function CodeExports({ orgId, sourceId }: { orgId: string; sourceId: string }) {
  const { t } = useTranslation();
  const [exports, setExports] = useState<CodeExport[] | null>(null);
  const [failed, setFailed] = useState("");
  useEffect(() => {
    let live = true;
    fetchExports(orgId, sourceId)
      .then((found) => live && setExports(found))
      .catch((e) => live && setFailed(String(e instanceof Error ? e.message : e)));
    return () => {
      live = false;
    };
  }, [orgId, sourceId]);
  return (
    <FormField size="full">
      <ExportList label={t("codeSourceFields.exports")} exports={exports} failed={failed} />
    </FormField>
  );
}
