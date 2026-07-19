// Copyright (c) 2026 Kenneth Stott
// Canary: a7a1cffa-2ad7-498e-80d5-682eef17ea5f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import CodeMirror from "@uiw/react-codemirror";
import { EditorView } from "@codemirror/view";
import { json } from "@codemirror/lang-json";
import { oneDark } from "@codemirror/theme-one-dark";
import { StreamLanguage } from "@codemirror/language";
import { useMantineColorScheme } from "@mantine/core";

// Minimal Protocol Buffers stream language for read-only highlighting. The classic token-style names
// (keyword/type/string/number/comment) map to standard highlight tags, so the shared theme colors it.
const protobuf = StreamLanguage.define<Record<string, never>>({
  name: "protobuf",
  token(stream) {
    if (stream.eatSpace()) return null;
    if (stream.match(/\/\/.*/) || stream.match(/\/\*[^]*?\*\//)) return "comment";
    if (stream.match(/"(?:[^"\\]|\\.)*"/) || stream.match(/'(?:[^'\\]|\\.)*'/)) return "string";
    if (stream.match(/\b\d+(?:\.\d+)?\b/)) return "number";
    if (
      stream.match(
        /\b(?:syntax|package|import|public|option|message|enum|service|rpc|returns|oneof|map|reserved|extend|extensions|group|stream|to|weak)\b/,
      )
    )
      return "keyword";
    if (
      stream.match(
        /\b(?:repeated|optional|required|double|float|int32|int64|uint32|uint64|sint32|sint64|fixed32|fixed64|sfixed32|sfixed64|bool|string|bytes)\b/,
      )
    )
      return "type";
    if (stream.match(/\b(?:true|false)\b/)) return "atom";
    stream.next();
    return null;
  },
});

const EXT_PROTO = [protobuf, EditorView.lineWrapping];
const EXT_JSON = [json(), EditorView.lineWrapping];

interface GrpcCodeViewProps {
  value: string;
  language: "proto" | "json";
  /** Shown (read-only) when value is empty — e.g. a loading/prompt line. */
  placeholder?: string;
  /** When provided, the pane is editable and emits changes. */
  onChange?: (value: string) => void;
  /** Placeholder shown inside an editable pane when empty. */
  editablePlaceholder?: string;
  "data-testid"?: string;
}

// CodeMirror pane for the gRPC panel — replaces a plain <pre> with syntax highlighting,
// line numbers, and code folding for the .proto definition, the JSON response, and the
// editable request body. Editable when onChange is supplied, read-only otherwise.
export function GrpcCodeView({
  value,
  language,
  placeholder,
  onChange,
  editablePlaceholder,
  "data-testid": dataTestid,
}: GrpcCodeViewProps) {
  const { colorScheme } = useMantineColorScheme();
  const editable = onChange !== undefined;
  return (
    <CodeMirror
      value={editable ? value : value || placeholder || ""}
      onChange={onChange}
      readOnly={!editable}
      editable={editable}
      placeholder={editable ? editablePlaceholder : undefined}
      height="100%"
      theme={colorScheme === "light" ? undefined : oneDark}
      extensions={language === "proto" ? EXT_PROTO : EXT_JSON}
      basicSetup={{
        lineNumbers: true,
        foldGutter: true,
        highlightActiveLine: editable,
        highlightActiveLineGutter: editable,
      }}
      style={{ flex: 1, minHeight: 0, overflow: "auto", fontSize: "0.8rem" }}
      data-testid={dataTestid}
    />
  );
}
