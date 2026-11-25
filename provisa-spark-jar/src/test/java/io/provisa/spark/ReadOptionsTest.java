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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** The options of a read, as tests/unit/test_provisa_pyspark.py holds provisa-pyspark's. */
class ReadOptionsTest {
  static final String URI = "grpc://127.0.0.1:8815";

  static Map<String, String> map(String... pairs) {
    Map<String, String> out = new LinkedHashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      out.put(pairs[i], pairs[i + 1]);
    }
    return out;
  }

  @Test
  void loadPathNamesTheTableAndTheCredentialRidesTheHeaders() {
    ReadOptions read =
        ReadOptions.parse(map("uri", URI, "token", "pat-1", "role", "clerk", "path", "sales.orders"));
    assertEquals(List.of("sales", "orders"), read.table());
    assertEquals("SELECT * FROM \"sales\".\"orders\"", read.statement());
    assertEquals(
        map("authorization", "Bearer pat-1", "x-provisa-role", "clerk"), read.headers());
  }

  @Test
  void theTableOptionNamesTheTableToo() {
    assertEquals(List.of("orders"), ReadOptions.parse(map("uri", URI, "table", "orders")).table());
  }

  @Test
  void optionNamesAreCaseInsensitiveAsSparkOptionsAre() {
    ReadOptions read = ReadOptions.parse(map("URI", URI, "Token", "t", "PATH", "orders"));
    assertEquals(URI, read.uri());
    assertEquals("t", read.token());
  }

  @Test
  void withoutATokenNoCredentialIsSent() {
    assertEquals(Map.of(), ReadOptions.parse(map("uri", URI, "path", "orders")).headers());
  }

  static Stream<Arguments> refusals() {
    return Stream.of(
        Arguments.of(
            map("path", "orders"),
            "option uri is required: Provisa's Flight SQL endpoint, grpc://host:port"),
        Arguments.of(
            map("uri", "http://h:8815", "path", "orders"),
            "option uri 'http://h:8815' is not a Flight endpoint; it starts grpc://, grpc+tcp://"
                + " or grpc+tls://"),
        Arguments.of(
            map("uri", URI), "no table is named: spark.read.format(\"provisa\").load(\"<table>\")"),
        Arguments.of(
            map("uri", URI, "path", "a", "table", "b"),
            "the table is named twice: give load's path or the table option, not both"),
        Arguments.of(
            map("uri", URI, "path", "orders", "tokn", "t", "Zone", "x"),
            "unknown option(s) tokn, zone; a Provisa read takes uri, token, role and the table"
                + " (load's path or the table option)"),
        Arguments.of(map("uri", URI, "path", "orders", "token", " "), "option token is empty"),
        Arguments.of(map("uri", URI, "path", "orders", "role", ""), "option role is empty"));
  }

  @ParameterizedTest
  @MethodSource("refusals")
  void optionsThatNameNoOneReadAreRefusedByName(Map<String, String> options, String message) {
    ReadOptions.OptionError error =
        assertThrows(ReadOptions.OptionError.class, () -> ReadOptions.parse(options));
    assertEquals(message, error.getMessage());
  }

  static Stream<Arguments> names() {
    return Stream.of(
        Arguments.of("orders", List.of("orders")),
        Arguments.of("sales.orders", List.of("sales", "orders")),
        Arguments.of("\"sales.eu\".orders", List.of("sales.eu", "orders")),
        Arguments.of("sales.\"say \"\"hi\"\"\"", List.of("sales", "say \"hi\"")));
  }

  @ParameterizedTest
  @MethodSource("names")
  void aDoubleQuotedPartKeepsItsDotsAndQuotes(String name, List<String> parts) {
    assertEquals(parts, ReadOptions.tableParts(name));
  }

  @ParameterizedTest
  @ValueSource(strings = {"sales..orders", "sales.", "\"open", "a\"b", "\"a\"b"})
  void aMalformedTableNameIsRefused(String name) {
    assertThrows(ReadOptions.OptionError.class, () -> ReadOptions.tableParts(name));
  }

  @Test
  void eachMalformedNameIsRefusedForItsReason() {
    assertEquals(
        "table 'sales..orders' has an empty part", refusal("sales..orders"));
    assertEquals("table '\"open' has an unclosed double quote", refusal("\"open"));
    assertEquals(
        "table 'a\"b' has a double quote inside an unquoted part", refusal("a\"b"));
    assertEquals(
        "table '\"a\"b': a quoted part is followed by 'b', not a dot", refusal("\"a\"b"));
  }

  private static String refusal(String name) {
    return assertThrows(ReadOptions.OptionError.class, () -> ReadOptions.tableParts(name))
        .getMessage();
  }

  @Test
  void aQuoteInAPartIsDoubledInTheStatement() {
    ReadOptions read = ReadOptions.parse(map("uri", URI, "path", "sales.\"o\"\"rders\""));
    assertEquals("SELECT * FROM \"sales\".\"o\"\"rders\"", read.statement());
  }

  @Test
  void aTextWithASingleQuoteIsQuotedAsPythonQuotesIt() {
    assertEquals("\"it's\"", PyRepr.of("it's"));
    assertEquals("'say \"hi\"'", PyRepr.of("say \"hi\""));
    assertTrue(PyRepr.of("a'\"b").equals("'a\\'\"b'"));
  }
}
