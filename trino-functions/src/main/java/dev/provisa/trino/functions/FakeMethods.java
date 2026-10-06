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
import net.datafaker.Faker;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.BiFunction;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * This engine's implementation of the fake methods a column may name (REQ-1494). A method is
 * named as Provisa names it; this engine's generator, seeded by the value's digest, makes its
 * value. A fake that is not stable may differ from another engine's -- it is consistent within this
 * engine only. A method this engine has no implementation of, or arguments it cannot take, refuse
 * the statement naming the method.
 */
final class FakeMethods
{
    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Methods named here directly: those whose provider method has another name, and those several
     * providers share a name for (a person's first name, not a pet's).
     */
    private static final Map<String, BiFunction<Faker, JsonNode, Object>> NAMED = Map.ofEntries(
            Map.entry("first_name", (f, a) -> f.name().firstName()),
            Map.entry("last_name", (f, a) -> f.name().lastName()),
            Map.entry("phone_number", (f, a) -> f.phoneNumber().phoneNumber()),
            Map.entry("street_address", (f, a) -> f.address().streetAddress()),
            Map.entry("city", (f, a) -> f.address().city()),
            Map.entry("state", (f, a) -> f.address().state()),
            Map.entry("country", (f, a) -> f.address().country()),
            Map.entry("word", (f, a) -> f.lorem().word()),
            Map.entry("sentence", (f, a) -> f.lorem().sentence()),
            Map.entry("email", (f, a) -> f.internet().emailAddress()),
            Map.entry("name", (f, a) -> f.name().fullName()),
            Map.entry("user_name", (f, a) -> f.internet().username()),
            Map.entry("postcode", (f, a) -> f.address().zipCode()),
            Map.entry("company", (f, a) -> f.company().name()),
            Map.entry("job", (f, a) -> f.job().title()),
            Map.entry("uuid4", (f, a) -> f.internet().uuid()),
            Map.entry("pyint", (f, a) -> f.number().numberBetween(
                    a.path("min_value").asLong(0), a.path("max_value").asLong(9999) + 1)));

    /** Every other no-argument provider method, by its name in snake case; ambiguous names left out. */
    private static final Map<String, Method[]> PROVIDED = index();

    private FakeMethods() {}

    static String value(String method, String argsJson, long digest)
    {
        JsonNode args = parse(method, argsJson);
        Faker faker = new Faker(new Random(digest));
        BiFunction<Faker, JsonNode, Object> named = NAMED.get(method);
        if (named != null) {
            return String.valueOf(named.apply(faker, args));
        }
        Method[] path = PROVIDED.get(method);
        if (path == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "this engine has no fake method " + method + "()");
        }
        if (!args.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "this engine's " + method + "() takes no arguments");
        }
        try {
            return String.valueOf(path[1].invoke(path[0].invoke(faker)));
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "fake method " + method + "() failed: " + e.getCause(), e);
        }
    }

    private static JsonNode parse(String method, String argsJson)
    {
        try {
            JsonNode node = JSON.readTree(argsJson);
            if (!node.isObject()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, method + "() arguments are not an object");
            }
            return node;
        }
        catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, method + "() arguments are not JSON", e);
        }
    }

    private static Map<String, Method[]> index()
    {
        Map<String, Method[]> found = new TreeMap<>();
        Map<String, Integer> seen = new TreeMap<>();
        for (Method provider : Faker.class.getMethods()) {
            if (provider.getParameterCount() != 0 || Modifier.isStatic(provider.getModifiers())
                    || !net.datafaker.providers.base.AbstractProvider.class.isAssignableFrom(provider.getReturnType())) {
                continue;
            }
            for (Method m : provider.getReturnType().getMethods()) {
                if (m.getParameterCount() != 0 || Modifier.isStatic(m.getModifiers())
                        || m.getDeclaringClass() == Object.class || m.getReturnType() == void.class) {
                    continue;
                }
                String snake = snake(m.getName());
                seen.merge(snake, 1, Integer::sum);
                found.put(snake, new Method[] {provider, m});
            }
        }
        seen.forEach((name, count) -> {
            if (count > 1) {
                found.remove(name);
            }
        });
        return Map.copyOf(found);
    }

    static String snake(String camel)
    {
        return camel.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase(java.util.Locale.ROOT);
    }
}
