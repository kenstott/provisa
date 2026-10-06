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

import io.trino.spi.Plugin;

import java.util.Set;

/** Provisa's fake functions (REQ-1494): {@code provisa_digest} and {@code provisa_fake_method}. */
public final class ProvisaFunctionsPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return Set.of(FakeFunctions.class);
    }
}
