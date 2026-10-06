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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * REQ-1494: the portable definition of stable fakes, the algorithm of provisa.fakes.portable over
 * the same versioned JSON (shipped in this jar from provisa/fakes/portable). Every step mirrors the
 * Python side exactly, so a stable fake is the same on every engine.
 */
final class PortableFakes
{
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Map<Integer, JsonNode> VERSIONS = new ConcurrentHashMap<>();

    private PortableFakes() {}

    private static final class Draws
    {
        private long s;

        Draws(long seed)
        {
            s = seed;
        }

        long next()
        {
            s += 0x9E3779B97F4A7C15L;
            long z = s;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }

        int below(int n)
        {
            return (int) Long.remainderUnsigned(next(), n);
        }
    }

    static JsonNode definition(int version)
    {
        return VERSIONS.computeIfAbsent(version, v -> {
            String path = "/provisa/fakes/portable/v" + v + ".json";
            try (InputStream in = PortableFakes.class.getResourceAsStream(path)) {
                if (in == null) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "there is no version " + v + " of the portable fake definition");
                }
                JsonNode doc = JSON.readTree(in);
                if (doc.get("version").asInt() != v) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, path + " holds version " + doc.get("version").asInt() + ", not " + v);
                }
                return doc;
            }
            catch (IOException e) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "cannot read " + path, e);
            }
        });
    }

    private static String fill(JsonNode doc, String method, Draws d)
    {
        JsonNode formats = doc.get("formats").get(method);
        String template = formats.get(d.below(formats.size())).asText();
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < template.length()) {
            char ch = template.charAt(i);
            if (ch == '{') {
                int end = template.indexOf('}', i);
                String name = template.substring(i + 1, end);
                JsonNode words = doc.get("lists").get(name);
                out.append(words != null ? words.get(d.below(words.size())).asText() : fill(doc, name, d));
                i = end + 1;
                continue;
            }
            if (ch == '#') {
                out.append((char) ('0' + d.below(10)));
            }
            else if (ch == '%') {
                out.append((char) ('1' + d.below(9)));
            }
            else if (ch == '?') {
                out.append((char) ('A' + d.below(26)));
            }
            else {
                out.append(ch);
            }
            i++;
        }
        String text = out.toString();
        JsonNode transform = doc.get("transforms").get(method);
        if (transform != null && transform.asText().equals("user")) {
            StringBuilder kept = new StringBuilder();
            for (char c : text.toLowerCase(java.util.Locale.ROOT).toCharArray()) {
                if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_') {
                    kept.append(c);
                }
            }
            text = kept.toString();
        }
        return text;
    }

    static String value(String method, int version, long digest)
    {
        JsonNode doc = definition(version);
        Draws d = new Draws(digest);
        if (method.equals("uuid4")) {
            long hi = d.next();
            long lo = d.next();
            hi = (hi & ~(0xFL << 12)) | (0x4L << 12);
            lo = (lo & ~(0x3L << 62)) | (0x2L << 62);
            String h = String.format("%016x%016x", hi, lo);
            return h.substring(0, 8) + "-" + h.substring(8, 12) + "-" + h.substring(12, 16) + "-" + h.substring(16, 20) + "-" + h.substring(20);
        }
        if (method.equals("sentence")) {
            JsonNode words = doc.get("lists").get("word");
            int n = 4 + d.below(6);
            StringBuilder text = new StringBuilder();
            for (int k = 0; k < n; k++) {
                if (k > 0) {
                    text.append(' ');
                }
                text.append(words.get(d.below(words.size())).asText());
            }
            return Character.toUpperCase(text.charAt(0)) + text.substring(1) + ".";
        }
        if (!doc.get("formats").has(method)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "version " + version + " of the portable fake definition has no " + method + "()");
        }
        return fill(doc, method, d);
    }
}
