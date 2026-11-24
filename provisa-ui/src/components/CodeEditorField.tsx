// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A form field whose value is a script: the app's code editor (the same CodeMirror and theme as
// the Data Science and Lineage editors) under a field label, with line numbers and highlighting,
// growing with its content. Its value and onChange are a Textarea's: the text, whole.

// Requirements: REQ-2225

import { useMemo } from "react";
import { Input, useMantineColorScheme } from "@mantine/core";
import CodeMirror, { EditorView } from "@uiw/react-codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import { python } from "@codemirror/lang-python";

/** The languages a script field highlights. */
const LANGUAGES = { python } as const;
export type CodeLanguage = keyof typeof LANGUAGES;

interface Props {
  label: string;
  value: string;
  onChange: (value: string) => void;
  language: CodeLanguage;
  required?: boolean;
  readOnly?: boolean;
  "data-testid"?: string;
}

export function CodeEditorField({
  label,
  value,
  onChange,
  language,
  required,
  readOnly,
  "data-testid": testId,
}: Props) {
  const { colorScheme } = useMantineColorScheme();
  const extensions = useMemo(() => [LANGUAGES[language](), EditorView.lineWrapping], [language]);
  return (
    <Input.Wrapper label={label} required={required}>
      <CodeMirror
        value={value}
        onChange={onChange}
        extensions={extensions}
        // The toggle only ever writes "light"/"dark" (theme/ColorSchemeToggle.tsx), as in
        // LineagePage: CodeMirror's own theme is the light one.
        theme={colorScheme === "light" ? undefined : oneDark}
        readOnly={readOnly}
        editable={!readOnly}
        minHeight="9rem"
        maxHeight="36rem"
        basicSetup={{ lineNumbers: true, highlightActiveLine: !readOnly, foldGutter: false }}
        style={{
          fontFamily: "var(--mantine-font-family-monospace)",
          fontSize: "0.85rem",
          border: "1px solid var(--mantine-color-default-border)",
          borderRadius: "var(--mantine-radius-sm)",
          overflow: "hidden",
        }}
        aria-label={label}
        data-testid={testId}
      />
    </Input.Wrapper>
  );
}
