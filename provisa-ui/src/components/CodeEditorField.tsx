// Copyright (c) 2026 Kenneth Stott
// Canary: a5063c55-53b0-4307-82ac-20e454ffac79
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

// Requirements: REQ-1984, REQ-2225

import { useMemo } from "react";
import { Input, useMantineColorScheme } from "@mantine/core";
import CodeMirror, { EditorView } from "@uiw/react-codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import { python } from "@codemirror/lang-python";
import { yaml } from "@codemirror/lang-yaml";

/** The languages a code field highlights: a connector's script (Python) and a code host's
 * Compose file (YAML). */
const LANGUAGES = { python, yaml } as const;
export type CodeLanguage = keyof typeof LANGUAGES;

interface EditorProps {
  value: string;
  language: CodeLanguage;
  label: string;
  /** Absent, the code is shown and not edited. */
  onChange?: (value: string) => void;
  "data-testid"?: string;
}

/** The editor itself: CodeMirror in the app's theme, with line numbers and highlighting. */
function CodeEditor({ value, language, label, onChange, "data-testid": testId }: EditorProps) {
  const { colorScheme } = useMantineColorScheme();
  const extensions = useMemo(() => [LANGUAGES[language](), EditorView.lineWrapping], [language]);
  const readOnly = onChange === undefined;
  return (
    <CodeMirror
      value={value}
      onChange={onChange}
      extensions={extensions}
      // The toggle only ever writes "light"/"dark" (theme/ColorSchemeToggle.tsx), as in
      // LineagePage: CodeMirror's own theme is the light one.
      theme={colorScheme === "light" ? undefined : oneDark}
      readOnly={readOnly}
      editable={!readOnly}
      minHeight={readOnly ? undefined : "9rem"}
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
  );
}

type Props = {
  label: string;
  value: string;
  language: CodeLanguage;
  required?: boolean;
  "data-testid"?: string;
} & (
  | { readOnly?: false; onChange: (value: string) => void }
  | { readOnly: true; onChange?: (value: string) => void }
);

export function CodeEditorField({
  label,
  value,
  onChange,
  language,
  required,
  readOnly,
  "data-testid": testId,
}: Props) {
  return (
    <Input.Wrapper label={label} required={required}>
      <CodeEditor
        label={label}
        value={value}
        language={language}
        onChange={readOnly ? undefined : onChange}
        data-testid={testId}
      />
    </Input.Wrapper>
  );
}

// REQ-1984: a form read shows the code in the editor too, read-only, under the read row's label.
CodeEditorField.readValue = (p: Record<string, unknown>) =>
  p.value ? (
    <CodeEditor
      label={p.label as string}
      value={p.value as string}
      language={p.language as CodeLanguage}
      data-testid={p["data-testid"] as string | undefined}
    />
  ) : null;
