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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The options of a Provisa read (REQ-2270), as provisa-pyspark's {@code options.py} takes them.
 *
 * <p>{@code spark.read.format("provisa").option("uri", ...).option("token", ...).load("sales.orders")}:
 *
 * <ul>
 *   <li>{@code uri}: Provisa's Arrow Flight SQL endpoint, {@code grpc://host:port} or
 *       {@code grpc+tls://host:port}.
 *   <li>{@code token}: the credential of the principal the read runs as, sent as
 *       {@code authorization: Bearer}.
 *   <li>{@code role}: a role to act as, honored only when the principal holds it.
 *   <li>the table, as {@code load}'s path or the {@code table} option, never both: its name as
 *       Provisa's SQL names it ({@code orders}, {@code sales.orders}; a part with a dot in it
 *       double-quoted).
 * </ul>
 *
 * <p>Any other option is refused by name, so a misspelt {@code token} is not a read as no one.
 */
public final class ReadOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final List<String> SCHEMES = List.of("grpc://", "grpc+tcp://", "grpc+tls://");
  private static final Set<String> KNOWN = Set.of("uri", "token", "role", "table", "path");

  /** A Provisa read's options do not name one read. */
  public static final class OptionError extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public OptionError(String message) {
      super(message);
    }
  }

  private final String uri;
  private final List<String> table;
  private final String token;
  private final String role;

  private ReadOptions(String uri, List<String> table, String token, String role) {
    this.uri = uri;
    this.table = Collections.unmodifiableList(new ArrayList<>(table));
    this.token = token;
    this.role = role;
  }

  public String uri() {
    return uri;
  }

  public List<String> table() {
    return table;
  }

  /** The credential, or null when the read names none. */
  public String token() {
    return token;
  }

  /** The role asked for, or null when the read asks for none. */
  public String role() {
    return role;
  }

  public static ReadOptions parse(Map<String, String> options) {
    Map<String, String> given = new TreeMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      given.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
    }
    Set<String> unknown = new TreeSet<>(given.keySet());
    unknown.removeAll(KNOWN);
    if (!unknown.isEmpty()) {
      throw new OptionError(
          "unknown option(s) "
              + String.join(", ", unknown)
              + "; a Provisa read takes uri, token, role and the table (load's path or the table"
              + " option)");
    }
    String uri = given.getOrDefault("uri", "").strip();
    if (uri.isEmpty()) {
      throw new OptionError(
          "option uri is required: Provisa's Flight SQL endpoint, grpc://host:port");
    }
    if (SCHEMES.stream().noneMatch(uri::startsWith)) {
      throw new OptionError(
          "option uri "
              + PyRepr.of(uri)
              + " is not a Flight endpoint; it starts grpc://, grpc+tcp:// or grpc+tls://");
    }
    String path = given.get("path");
    String table = given.get("table");
    if (truthy(path) && truthy(table)) {
      throw new OptionError(
          "the table is named twice: give load's path or the table option, not both");
    }
    String name = (truthy(path) ? path : truthy(table) ? table : "").strip();
    if (name.isEmpty()) {
      throw new OptionError("no table is named: spark.read.format(\"provisa\").load(\"<table>\")");
    }
    return new ReadOptions(
        uri, tableParts(name), present(given, "token"), present(given, "role"));
  }

  private static boolean truthy(String value) {
    return value != null && !value.isEmpty();
  }

  /** The statement whose rows are the table's: every column the principal is served. */
  public String statement() {
    List<String> quoted = new ArrayList<>();
    for (String part : table) {
      quoted.add(quoted(part));
    }
    return "SELECT * FROM " + String.join(".", quoted);
  }

  /** The call headers naming the caller: the credential, and the role it asks for. */
  public Map<String, String> headers() {
    Map<String, String> headers = new java.util.LinkedHashMap<>();
    if (token != null) {
      headers.put("authorization", "Bearer " + token);
    }
    if (role != null) {
      headers.put("x-provisa-role", role);
    }
    return headers;
  }

  /** The option's value; an option given empty is refused rather than read as absent. */
  private static String present(Map<String, String> given, String key) {
    if (!given.containsKey(key)) {
      return null;
    }
    String value = given.get(key).strip();
    if (value.isEmpty()) {
      throw new OptionError("option " + key + " is empty");
    }
    return value;
  }

  /** The parts of a dotted table name; a double-quoted part keeps its dots ({@code ""} is a quote). */
  public static List<String> tableParts(String name) {
    List<String> parts = new ArrayList<>();
    int i = 0;
    int n = name.length();
    while (true) {
      if (i < n && name.charAt(i) == '"') {
        i++;
        StringBuilder part = new StringBuilder();
        while (true) {
          if (i >= n) {
            throw new OptionError("table " + PyRepr.of(name) + " has an unclosed double quote");
          }
          if (name.charAt(i) == '"') {
            if (i + 1 < n && name.charAt(i + 1) == '"') {
              part.append('"');
              i += 2;
              continue;
            }
            i++;
            break;
          }
          part.append(name.charAt(i));
          i++;
        }
        parts.add(part.toString());
      } else {
        int end = name.indexOf('.', i);
        end = end == -1 ? n : end;
        String bare = name.substring(i, end);
        if (bare.indexOf('"') >= 0) {
          throw new OptionError(
              "table " + PyRepr.of(name) + " has a double quote inside an unquoted part");
        }
        parts.add(bare);
        i = end;
      }
      if (parts.get(parts.size() - 1).isEmpty()) {
        throw new OptionError("table " + PyRepr.of(name) + " has an empty part");
      }
      if (i == n) {
        return parts;
      }
      if (name.charAt(i) != '.') {
        throw new OptionError(
            "table "
                + PyRepr.of(name)
                + ": a quoted part is followed by "
                + PyRepr.of(String.valueOf(name.charAt(i)))
                + ", not a dot");
      }
      i++;
    }
  }

  private static String quoted(String part) {
    return '"' + part.replace("\"", "\"\"") + '"';
  }
}
