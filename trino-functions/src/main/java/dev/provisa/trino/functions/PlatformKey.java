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

import io.trino.spi.TrinoException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * Platform fake keys (REQ-1494), each in the file {@code <PROVISA_FAKE_KEY_DIR>/<fingerprint>.key}
 * the deployment mounts into the engine. A statement names the key by its fingerprint -- the first
 * sixteen hex digits of the SHA-256 of the key's hex text -- never by the key, so platforms sharing
 * this engine each reach their own.
 */
final class PlatformKey
{
    static final String KEY_DIR_ENV = "PROVISA_FAKE_KEY_DIR";

    private static final Map<String, byte[]> KEYS = new ConcurrentHashMap<>();

    private PlatformKey() {}

    static byte[] get(String fingerprint)
    {
        byte[] key = KEYS.get(fingerprint);
        if (key != null) {
            return key;
        }
        key = read(fingerprint);
        KEYS.put(fingerprint, key);
        return key;
    }

    private static byte[] read(String fingerprint)
    {
        if (!fingerprint.matches("[0-9a-f]{16}")) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "not a fake key fingerprint: " + fingerprint);
        }
        String dir = System.getenv(KEY_DIR_ENV);
        if (dir == null || dir.isBlank()) {
            throw new TrinoException(CONFIGURATION_INVALID, KEY_DIR_ENV + " is not set: the engine holds no platform fake key");
        }
        Path path = Path.of(dir, fingerprint + ".key");
        String hex;
        try {
            hex = Files.readString(path, StandardCharsets.UTF_8).strip();
        }
        catch (IOException e) {
            // The statement names a key this engine was not given: the statement's error.
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "this engine holds no fake key " + fingerprint, e);
        }
        if (!fingerprint(hex).equals(fingerprint)) {
            throw new TrinoException(CONFIGURATION_INVALID, "the fake key file " + path + " holds another key");
        }
        return HexFormat.of().parseHex(hex);
    }

    static String fingerprint(String hexKey)
    {
        try {
            byte[] sha = MessageDigest.getInstance("SHA-256").digest(hexKey.getBytes(StandardCharsets.US_ASCII));
            return HexFormat.of().formatHex(sha).substring(0, 16);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
