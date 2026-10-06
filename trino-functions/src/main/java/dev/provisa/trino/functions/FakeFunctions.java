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

    @ScalarFunction("provisa_fake_method")
    @Description("The named fake method's value for a value's digest")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fakeMethod(
            @SqlType(StandardTypes.VARCHAR) Slice method,
            @SqlType(StandardTypes.VARCHAR) Slice args,
            @SqlType(StandardTypes.BIGINT) long digest)
    {
        return Slices.utf8Slice(FakeMethods.value(method.toStringUtf8(), args.toStringUtf8(), digest));
    }
}
