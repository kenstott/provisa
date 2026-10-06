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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

/** The fake functions inside the Trino engine (REQ-1494). */
public final class FakeFunctions
{
    private FakeFunctions() {}

    @ScalarFunction("provisa_digest")
    @Description("The keyed digest of a value's text under the platform fake key a fingerprint names")
    @SqlType(StandardTypes.BIGINT)
    public static long digest(
            @SqlType(StandardTypes.VARCHAR) Slice fingerprint,
            @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        return digest(PlatformKey.get(fingerprint.toStringUtf8()), value.getBytes());
    }

    /** The first eight bytes of HMAC-SHA256 under {@code key}, read as a signed big-endian long. */
    static long digest(byte[] key, byte[] text)
    {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return ByteBuffer.wrap(mac.doFinal(text), 0, 8).getLong();
        }
        catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is unavailable", e);
        }
    }

    @ScalarFunction("provisa_digest_tag")
    @Description("A digest as a short tag: base 36 of its unsigned value")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice digestTag(@SqlType(StandardTypes.BIGINT) long digest)
    {
        return Slices.utf8Slice(Long.toUnsignedString(digest, 36));
    }

    @ScalarFunction("provisa_fake_method")
    @Description("The named fake method's value for a value's digest")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice fakeMethod(
            @SqlType(StandardTypes.VARCHAR) Slice method,
            @SqlType(StandardTypes.VARCHAR) Slice args,
            @SqlType(StandardTypes.BIGINT) long digest,
            @SqlType(StandardTypes.BIGINT) long defHash)
    {
        String value = FakeMethods.value(method.toStringUtf8(), args.toStringUtf8(), seed(digest, defHash));
        return value == null ? null : Slices.utf8Slice(value);
    }

    @ScalarFunction("provisa_stable_fake")
    @Description("The stable fake of a method for a value's digest, by the portable definition's version")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stableFake(
            @SqlType(StandardTypes.VARCHAR) Slice method,
            @SqlType(StandardTypes.INTEGER) long version,
            @SqlType(StandardTypes.BIGINT) long digest,
            @SqlType(StandardTypes.BIGINT) long defHash)
    {
        return Slices.utf8Slice(PortableFakes.value(method.toStringUtf8(), (int) version, seed(digest, defHash)));
    }

    @ScalarFunction("provisa_seed")
    @Description("A value's seed under a fake's definition: its digest mixed with the definition hash")
    @SqlType(StandardTypes.BIGINT)
    public static long seedFunction(
            @SqlType(StandardTypes.BIGINT) long digest,
            @SqlType(StandardTypes.BIGINT) long defHash)
    {
        return seed(digest, defHash);
    }

    /**
     * REQ-1494: a method's seed for one value -- splitmix64's mix of the digest XOR the definition
     * hash of the column's fake, as provisa.fakes.digest.seed computes it.
     */
    static long seed(long digest, long defHash)
    {
        long z = (digest ^ defHash) + 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
