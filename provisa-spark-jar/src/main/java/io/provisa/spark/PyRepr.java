// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-2270

package io.provisa.spark;

/**
 * A string quoted as Python's {@code repr} quotes it, so a refusal reads the same from this jar as
 * from provisa-pyspark: single quotes, or double quotes when the text holds a single quote and no
 * double one.
 */
final class PyRepr {
  private PyRepr() {}

  static String of(String text) {
    char quote = text.indexOf('\'') >= 0 && text.indexOf('"') < 0 ? '"' : '\'';
    StringBuilder out = new StringBuilder().append(quote);
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '\\' || c == quote) {
        out.append('\\').append(c);
      } else if (c == '\n') {
        out.append("\\n");
      } else if (c == '\r') {
        out.append("\\r");
      } else if (c == '\t') {
        out.append("\\t");
      } else if (c < 0x20 || c == 0x7f) {
        out.append(String.format("\\x%02x", (int) c));
      } else {
        out.append(c);
      }
    }
    return out.append(quote).toString();
  }
}
