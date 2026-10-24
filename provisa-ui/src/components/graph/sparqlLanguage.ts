// Copyright (c) 2026 Kenneth Stott
// Canary: 1c6e8b34-2f97-4a05-b3d1-8e4a6c2f0b79
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135 phase 5: SPARQL's highlighting in the query bar -- keywords, variables, IRIs, prefixed
// names, literals and comments -- as a stream language, the editor's own mechanism.

import { StreamLanguage, type StreamParser } from "@codemirror/language";

const KEYWORDS = new Set(
  (
    "select construct ask describe where filter optional union minus graph service bind values " +
    "prefix base distinct reduced from named order by asc desc limit offset group having as not " +
    "exists in a true false"
  ).split(" "),
);

const parser: StreamParser<null> = {
  token(stream) {
    if (stream.eatSpace()) return null;
    if (stream.match("#")) {
      stream.skipToEnd();
      return "comment";
    }
    if (stream.match(/^<[^\s>]*>/)) return "link";
    if (stream.match(/^[?$][A-Za-z_]\w*/)) return "variableName";
    if (stream.match(/^"(?:[^"\\]|\\.)*"/) || stream.match(/^'(?:[^'\\]|\\.)*'/)) return "string";
    if (stream.match(/^-?\d+(\.\d+)?/)) return "number";
    if (stream.match(/^[A-Za-z_][\w-]*:[\w.-]*/)) return "atom";
    if (stream.match(/^[A-Za-z_]\w*/)) {
      return KEYWORDS.has(stream.current().toLowerCase()) ? "keyword" : null;
    }
    stream.next();
    return null;
  },
};

export const sparqlLanguage = StreamLanguage.define(parser);
