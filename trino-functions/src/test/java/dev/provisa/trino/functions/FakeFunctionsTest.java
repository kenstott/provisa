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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FakeFunctionsTest
{
    private static final byte[] KEY = "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk".getBytes(StandardCharsets.UTF_8);

    /** The value provisa.fakes.digest.digest(b"k" * 32, "ann@example.com") gives. */
    @Test
    void theDigestIsThePublishedDefinition()
    {
        assertEquals(5975387752016995628L, FakeFunctions.digest(KEY, "ann@example.com".getBytes(StandardCharsets.UTF_8)));
    }

    /** The value provisa.fakes.digest.fingerprint(b"k" * 32) gives. */
    @Test
    void theFingerprintIsThePublishedDefinition()
    {
        assertEquals("1d169c852c85cba1", PlatformKey.fingerprint(java.util.HexFormat.of().formatHex(KEY)));
    }

    @Test
    void oneValueGivesOneFake()
    {
        String a = FakeMethods.value("email", "{}", 42L);
        assertEquals(a, FakeMethods.value("email", "{}", 42L));
        assertNotEquals(a, FakeMethods.value("email", "{}", 43L));
        assertTrue(a.contains("@"));
        assertEquals("3", FakeMethods.value("pyint", "{\"min_value\": 3, \"max_value\": 3}", 1L));
        assertTrue(!FakeMethods.value("first_name", "{}", 7L).isEmpty());
    }
}
