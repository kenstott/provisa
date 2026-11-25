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

// Requirements: REQ-1984, REQ-2225, REQ-2115

import { useMemo, type ReactNode } from "react";
import { Input, useMantineColorScheme } from "@mantine/core";
import CodeMirror, { EditorView } from "@uiw/react-codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import { StreamLanguage } from "@codemirror/language";
import { json } from "@codemirror/lang-json";
import { python } from "@codemirror/lang-python";
import { PostgreSQL, sql } from "@codemirror/lang-sql";
import { yaml } from "@codemirror/lang-yaml";
import { turtle } from "@codemirror/legacy-modes/mode/turtle";
import { codeFieldMark, type CodeFieldName } from "../mcp/codeFields";

const turtleLanguage = StreamLanguage.define(turtle);

/** The languages a code field highlights: a connector's script (Python), a code host's Compose
 * file and an OpenAPI spec (YAML or JSON), a command's SQL, and a reasoner's axioms (Turtle). */
const LANGUAGES = {
  python,
  yaml,
  json,
  sql: () => sql({ dialect: PostgreSQL }),
  turtle: () => turtleLanguage,
} as const;
export type CodeLanguage = keyof typeof LANGUAGES;

interface EditorProps {
  value: string;
  language: CodeLanguage;
  /** Its accessible name, on the editor's textbox. */
  name: CodeFieldName;
  /** Absent, the code is shown and not edited. */
  onChange?: (value: string) => void;
  placeholder?: string;
  "data-testid"?: string;
}

/** The editor itself: CodeMirror in the app's theme, with line numbers and highlighting. */
function CodeEditor({
  value,
  language,
  name,
  onChange,
  placeholder,
  "data-testid": testId,
}: EditorProps) {
  const { colorScheme } = useMantineColorScheme();
  // REQ-2115: marked with its name and language, so Polly reads and fills it (mcp/codeFields.ts).
  // By value: a caller builds the name on every render, and only a change reconfigures the editor.
  const nameKey = JSON.stringify(name);
  const extensions = useMemo(
    () => [
      LANGUAGES[language](),
      EditorView.lineWrapping,
      codeFieldMark(JSON.parse(nameKey) as CodeFieldName, language),
    ],
    [language, nameKey],
  );
  const readOnly = onChange === undefined;
  return (
    <CodeMirror
      value={value}
      onChange={readOnly ? undefined : (text) => onChange(text)}
      extensions={extensions}
      // The toggle only ever writes "light"/"dark" (theme/ColorSchemeToggle.tsx), as in
      // LineagePage: CodeMirror's own theme is the light one.
      theme={colorScheme === "light" ? undefined : oneDark}
      readOnly={readOnly}
      placeholder={placeholder}
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
      data-testid={testId}
    />
  );
}

type Props = {
  /** Text, or (from a FormField with help) a label node whose text `aria-labelledby` names. */
  label: ReactNode;
  "aria-labelledby"?: string;
  "aria-description"?: string;
  value: string;
  language: CodeLanguage;
  required?: boolean;
  placeholder?: string;
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
  placeholder,
  "aria-labelledby": labelledBy,
  "aria-description": description,
  "data-testid": testId,
}: Props) {
  let name: CodeFieldName;
  if (labelledBy !== undefined) name = { labelledBy, description };
  else if (typeof label === "string") name = { label, description };
  else throw new Error("CodeEditorField: a label that is not text needs aria-labelledby");
  return (
    <Input.Wrapper label={label} required={required}>
      <CodeEditor
        name={name}
        value={value}
        language={language}
        onChange={readOnly ? undefined : onChange}
        placeholder={placeholder}
        data-testid={testId}
      />
    </Input.Wrapper>
  );
}

// REQ-1984: a form read shows the code in the editor too, read-only, under the read row's label.
CodeEditorField.readValue = (p: Record<string, unknown>) =>
  p.value ? (
    <CodeEditor
      name={{ label: p.label as string }}
      value={p.value as string}
      language={p.language as CodeLanguage}
      data-testid={p["data-testid"] as string | undefined}
    />
  ) : null;
