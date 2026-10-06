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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * REQ-1494: the portable definition gives what provisa.fakes.portable.stable_fake gives. The
 * values are the Python side's for SEED (tests/unit/test_fake_portable.py holds the two together).
 */
class PortableFakesTest
{
    private static final long SEED = -4242424242424242L;

    private static final String[][] PINNED = {
            {"city", "Latoyafurt"},
            {"company", "Kramer-Berg"},
            {"country", "Saudi Arabia"},
            {"email", "blake.higgins@gmail.com"},
            {"first_name", "Latoya"},
            {"job", "Chief Operating Officer"},
            {"last_name", "Kramer"},
            {"name", "Latoya Berg"},
            {"phone_number", "964-715-5231"},
            {"postcode", "36481"},
            {"sentence", "Pick those enjoy least call around want from."},
            {"state", "Mississippi"},
            {"street_address", "348 Collier Common"},
            {"user_name", "latoya64"},
            {"uuid4", "eeeb276d-7830-4f56-8bfd-6cb84c1bad31"},
            {"word", "pick"},
    };

    @Test
    void everyStableMethodGivesWhatThePythonSideGives()
    {
        for (String[] pinned : PINNED) {
            assertEquals(pinned[1], PortableFakes.value(pinned[0], 1, SEED), pinned[0]);
        }
    }
}
