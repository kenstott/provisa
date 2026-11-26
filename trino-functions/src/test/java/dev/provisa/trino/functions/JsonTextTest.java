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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonTextTest
{
    /**
     * REQ-2279: each input's canonical text is the one provisa.fakes.digest.canonical_json gives
     * (tests/unit/test_fake_json_digest.py reads the same file), so a JSON column digests alike.
     */
    @Test
    void theCanonicalTextIsThePublishedDefinition()
            throws IOException
    {
        JsonNode vectors;
        try (InputStream in = JsonTextTest.class.getResourceAsStream("/json_canonical.json")) {
            vectors = new ObjectMapper().readTree(in);
        }
        assertTrue(vectors.size() >= 10);
        for (JsonNode pair : vectors) {
            assertEquals(pair.get(1).textValue(), JsonText.canonical(pair.get(0).textValue()), pair.get(0).textValue());
        }
    }

    @Test
    void textThatIsNotJsonIsRefused()
    {
        assertThrows(TrinoException.class, () -> JsonText.canonical("{\"a\": NaN}"));
        assertThrows(TrinoException.class, () -> JsonText.canonical("1 2"));
    }
}
