// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-788: a files source may be registered as ONE logical table over a glob of files, with an
// optional column carrying each row's file path. Extracted from RegisterTableForm so that form
// stays within its line budget.

import { Fragment } from "react";
import { Text, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";

export function FilesGlobFieldset({
  fileGlob,
  setFileGlob,
  sourceFileColumn,
  setSourceFileColumn,
}: {
  fileGlob: string;
  setFileGlob: (v: string) => void;
  sourceFileColumn: string;
  setSourceFileColumn: (v: string) => void;
}) {
  const { t } = useTranslation();
  return (
    <Fragment>
      <TextInput
        label={
          <>
            {t("registerTableForm.fileGlobLabel")}{" "}
            <Text span fw="normal" c="dimmed" fz="xs">
              {t("registerTableForm.fileGlobHint")}
            </Text>
          </>
        }
        placeholder={t("registerTableForm.fileGlobPlaceholder")}
        value={fileGlob}
        onChange={(e) => setFileGlob(e.currentTarget.value)}
        data-testid="register-table-file-glob"
      />
      <TextInput
        label={
          <>
            {t("registerTableForm.sourceFileColumnLabel")}{" "}
            <Text span fw="normal" c="dimmed" fz="xs">
              {t("registerTableForm.sourceFileColumnHint")}
            </Text>
          </>
        }
        placeholder={t("registerTableForm.sourceFileColumnPlaceholder")}
        value={sourceFileColumn}
        onChange={(e) => setSourceFileColumn(e.currentTarget.value)}
        disabled={!fileGlob.trim()}
        data-testid="register-table-source-file-column"
      />
    </Fragment>
  );
}
