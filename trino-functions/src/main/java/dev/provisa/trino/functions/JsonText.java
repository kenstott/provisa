/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package dev.provisa.trino.functions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * REQ-2279: a JSON value's canonical text, the one a JSON column is digested as on every engine --
 * provisa.fakes.digest.canonical_json's definition: minified; object keys sorted by code point,
 * the last of a repeated key kept; a number as its exact value in plain digits, no exponent and no
 * trailing zeros ("0" for any zero); a string with only '"', '\' and the control characters
 * escaped (\b \f \n \r \t, any other as backslash-u and four lowercase hex digits), every other character as itself.
 */
public final class JsonText
{
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .nodeFactory(JsonNodeFactory.withExactBigDecimals(true))
            .build();

    private JsonText() {}

    @ScalarFunction("provisa_json_text")
    @Description("A JSON value's canonical text: minified, keys sorted, numbers in plain digits")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ofJson(@SqlType(StandardTypes.JSON) Slice value)
    {
        return Slices.utf8Slice(canonical(value.toStringUtf8()));
    }

    @ScalarFunction("provisa_json_text")
    @Description("JSON text's canonical text: minified, keys sorted, numbers in plain digits")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice ofText(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        return Slices.utf8Slice(canonical(value.toStringUtf8()));
    }

    static String canonical(String text)
    {
        JsonNode node;
        try {
            node = MAPPER.readTree(text);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "not JSON: " + e.getOriginalMessage(), e);
        }
        StringBuilder out = new StringBuilder();
        write(node, out);
        return out.toString();
    }

    private static void write(JsonNode node, StringBuilder out)
    {
        if (node.isObject()) {
            List<Map.Entry<String, JsonNode>> fields = new ArrayList<>(node.properties());
            fields.sort((a, b) -> byCodePoint(a.getKey(), b.getKey()));
            out.append('{');
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) {
                    out.append(',');
                }
                string(fields.get(i).getKey(), out);
                out.append(':');
                write(fields.get(i).getValue(), out);
            }
            out.append('}');
        }
        else if (node.isArray()) {
            out.append('[');
            for (int i = 0; i < node.size(); i++) {
                if (i > 0) {
                    out.append(',');
                }
                write(node.get(i), out);
            }
            out.append(']');
        }
        else if (node.isNumber()) {
            BigDecimal value = node.decimalValue();
            out.append(value.signum() == 0 ? "0" : value.stripTrailingZeros().toPlainString());
        }
        else if (node.isTextual()) {
            string(node.textValue(), out);
        }
        else if (node.isBoolean()) {
            out.append(node.booleanValue() ? "true" : "false");
        }
        else {
            out.append("null");
        }
    }

    /** Code-point order, as Python sorts strings (String.compareTo is UTF-16 unit order). */
    static int byCodePoint(String a, String b)
    {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int ca = a.codePointAt(i);
            int cb = b.codePointAt(j);
            if (ca != cb) {
                return Integer.compare(ca, cb);
            }
            i += Character.charCount(ca);
            j += Character.charCount(cb);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }

    private static void string(String s, StringBuilder out)
    {
        out.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    }
                    else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
