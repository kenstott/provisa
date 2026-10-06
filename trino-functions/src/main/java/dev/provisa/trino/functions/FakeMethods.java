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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.trino.spi.TrinoException;
import net.datafaker.Faker;
import net.datafaker.service.RandomService;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiFunction;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * This engine's implementation of every fake method a column may name (REQ-1494). A method is
 * named as Provisa names it; this engine's generator, seeded by the value's digest, makes its
 * value. A fake that is not stable may differ from another engine's -- it is consistent within this
 * engine only. Every method whose values a column can hold is here (the Python side's set, enforced
 * by tests/integration/test_fake_method_coverage_e2e.py); a method or an argument this engine has
 * no implementation of refuses the statement naming it.
 */
final class FakeMethods
{
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final LocalDate TODAY = LocalDate.of(2026, 1, 1);

    private record Ctx(Faker f, RandomService r, JsonNode a)
    {
        String text(String key, String fallbackWhenUnset)
        {
            // An argument the declaration did not give takes the method's own default.
            return a.has(key) ? a.get(key).asText() : fallbackWhenUnset;
        }

        long num(String key, long methodDefault)
        {
            return a.has(key) ? a.get(key).asLong() : methodDefault;
        }

        String pick(String... options)
        {
            return options[r.nextInt(options.length)];
        }

        String digits(int n)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < n; i++) {
                sb.append((char) ('0' + r.nextInt(10)));
            }
            return sb.toString();
        }

        String letters(int n, boolean upper)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < n; i++) {
                sb.append((char) ((upper ? 'A' : 'a') + r.nextInt(26)));
            }
            return sb.toString();
        }

        LocalDate date(LocalDate from, LocalDate to)
        {
            return from.plusDays(r.nextLong(Math.max(1, to.toEpochDay() - from.toEpochDay() + 1)));
        }

        LocalDateTime stamp(LocalDate from, LocalDate to)
        {
            return date(from, to).atStartOfDay().plusSeconds(r.nextLong(86400));
        }

        LocalDateTime between(LocalDateTime from, LocalDateTime to)
        {
            long span = Math.max(1, java.time.Duration.between(from, to).getSeconds() + 1);
            return from.plusSeconds(r.nextLong(span));
        }

        boolean flag(String key, boolean methodDefault)
        {
            return a.has(key) ? a.get(key).asBoolean() : methodDefault;
        }

        /** A moment as the Python side names it: today, now, +30d, -30y, -1y6M, or an ISO date. */
        LocalDateTime moment(String key, String methodDefault)
        {
            return parseMoment(a.has(key) && !a.get(key).isNull() ? a.get(key).asText() : methodDefault);
        }

        String elementText(JsonNode n)
        {
            return n.isTextual() ? n.asText() : n.toString();
        }

        List<String> elements()
        {
            java.util.List<String> out = new java.util.ArrayList<>();
            if (a.has("elements")) {
                a.get("elements").forEach(n -> out.add(elementText(n)));
            }
            else {
                out.addAll(List.of("a", "b", "c"));
            }
            return out;
        }

        String fill(String template, String letters)
        {
            StringBuilder sb = new StringBuilder();
            for (char ch : template.toCharArray()) {
                if (ch == '#') {
                    sb.append((char) ('0' + r.nextInt(10)));
                }
                else if (ch == '%') {
                    sb.append((char) ('1' + r.nextInt(9)));
                }
                else if (ch == '?') {
                    sb.append(letters.charAt(r.nextInt(letters.length())));
                }
                else {
                    sb.append(ch);
                }
            }
            return sb.toString();
        }
    }

    private static final LocalDateTime NOW = TODAY.atStartOfDay();
    private static final String ASCII_LETTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    static LocalDateTime parseMoment(String spec)
    {
        String s = spec.strip();
        if (s.equals("today") || s.equals("now")) {
            return NOW;
        }
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("([-+]?\\d+)([yMwdhms])").matcher(s);
        if (s.matches("([-+]?\\d+[yMwdhms])+")) {
            LocalDateTime t = NOW;
            while (m.find()) {
                long n = Long.parseLong(m.group(1));
                t = switch (m.group(2)) {
                    case "y" -> t.plusYears(n);
                    case "M" -> t.plusMonths(n);
                    case "w" -> t.plusWeeks(n);
                    case "d" -> t.plusDays(n);
                    case "h" -> t.plusHours(n);
                    case "m" -> t.plusMinutes(n);
                    default -> t.plusSeconds(n);
                };
            }
            return t;
        }
        try {
            return s.length() <= 10 ? LocalDate.parse(s).atStartOfDay() : LocalDateTime.parse(s.replace(' ', 'T'));
        }
        catch (java.time.format.DateTimeParseException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "not a date or time: " + spec, e);
        }
    }

    /** A strftime pattern as a Java formatter. */
    static DateTimeFormatter strftime(String pattern)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            if (ch == '%' && i + 1 < pattern.length()) {
                char d = pattern.charAt(++i);
                sb.append(switch (d) {
                    case 'Y' -> "yyyy";
                    case 'y' -> "yy";
                    case 'm' -> "MM";
                    case 'd' -> "dd";
                    case 'H' -> "HH";
                    case 'I' -> "hh";
                    case 'M' -> "mm";
                    case 'S' -> "ss";
                    case 'p' -> "a";
                    case 'b' -> "MMM";
                    case 'B' -> "MMMM";
                    case 'a' -> "EEE";
                    case 'A' -> "EEEE";
                    case 'j' -> "DDD";
                    case '%' -> "'%'";
                    default -> throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "this engine has no date directive %" + d);
                });
            }
            else if (Character.isLetter(ch)) {
                sb.append('\'').append(ch).append('\'');
            }
            else if (ch == '\'') {
                sb.append("''");
            }
            else {
                sb.append(ch);
            }
        }
        return DateTimeFormatter.ofPattern(sb.toString(), Locale.US);
    }

    /**
     * Arguments each method accepts, on every engine (provisa.fakes.methods.ARGUMENTS, checked
     * against this map by tests/unit/test_fake_method_arguments.py); any other argument refuses the
     * statement by name.
     */
    static final Map<String, List<String>> ARGUMENTS = Map.ofEntries(
            Map.entry("bothify", List.of("text", "letters")),
            Map.entry("numerify", List.of("text")),
            Map.entry("lexify", List.of("text", "letters")),
            Map.entry("hexify", List.of("text", "upper")),
            Map.entry("pyint", List.of("min_value", "max_value", "step")),
            Map.entry("random_int", List.of("min", "max", "step")),
            Map.entry("random_number", List.of("digits", "fix_len")),
            Map.entry("pyfloat", List.of("left_digits", "right_digits", "positive", "min_value", "max_value")),
            Map.entry("pydecimal", List.of("left_digits", "right_digits", "positive", "min_value", "max_value")),
            Map.entry("pystr", List.of("min_chars", "max_chars", "prefix", "suffix")),
            Map.entry("password", List.of("length", "special_chars", "digits", "upper_case", "lower_case")),
            Map.entry("nic_handle", List.of("suffix")),
            Map.entry("nic_handles", List.of("count", "suffix")),
            Map.entry("date", List.of("pattern", "end_datetime")),
            Map.entry("time", List.of("pattern", "end_datetime")),
            Map.entry("date_object", List.of("end_datetime")),
            Map.entry("date_time", List.of("end_datetime")),
            Map.entry("date_time_ad", List.of("start_datetime", "end_datetime")),
            Map.entry("iso8601", List.of("end_datetime", "sep")),
            Map.entry("boolean", List.of("chance_of_getting_true")),
            Map.entry("pybool", List.of("truth_probability")),
            Map.entry("random_element", List.of("elements")),
            Map.entry("date_between", List.of("start_date", "end_date")),
            Map.entry("date_time_between", List.of("start_date", "end_date")),
            Map.entry("date_between_dates", List.of("date_start", "date_end")),
            Map.entry("date_time_between_dates", List.of("datetime_start", "datetime_end")),
            Map.entry("future_date", List.of("end_date")),
            Map.entry("future_datetime", List.of("end_date")),
            Map.entry("past_date", List.of("start_date")),
            Map.entry("past_datetime", List.of("start_date")),
            Map.entry("date_of_birth", List.of("minimum_age", "maximum_age")),
            Map.entry("date_this_century", List.of("before_today", "after_today")),
            Map.entry("date_this_decade", List.of("before_today", "after_today")),
            Map.entry("date_this_year", List.of("before_today", "after_today")),
            Map.entry("date_this_month", List.of("before_today", "after_today")),
            Map.entry("date_time_this_century", List.of("before_now", "after_now")),
            Map.entry("date_time_this_decade", List.of("before_now", "after_now")),
            Map.entry("date_time_this_year", List.of("before_now", "after_now")),
            Map.entry("date_time_this_month", List.of("before_now", "after_now")),
            Map.entry("unix_time", List.of("start_datetime", "end_datetime")),
            Map.entry("words", List.of("nb", "unique")),
            Map.entry("sentences", List.of("nb")),
            Map.entry("paragraphs", List.of("nb")),
            Map.entry("texts", List.of("nb_texts", "max_nb_chars")),
            Map.entry("sentence", List.of("nb_words")),
            Map.entry("paragraph", List.of("nb_sentences")),
            Map.entry("text", List.of("max_nb_chars")),
            Map.entry("random_letters", List.of("length")),
            Map.entry("random_choices", List.of("elements", "length")),
            Map.entry("random_elements", List.of("elements", "length", "unique")),
            Map.entry("random_sample", List.of("elements", "length")));

    private static final Map<String, BiFunction<Faker, JsonNode, Object>> METHODS = methods();

    private FakeMethods() {}

    static String value(String method, String argsJson, long digest)
    {
        BiFunction<Faker, JsonNode, Object> m = METHODS.get(method);
        if (m == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "this engine has no fake method " + method + "()");
        }
        JsonNode args = parse(method, argsJson);
        List<String> accepted = ARGUMENTS.getOrDefault(method, List.of());
        for (Iterator<String> it = args.fieldNames(); it.hasNext(); ) {
            String name = it.next();
            if (!accepted.contains(name)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "this engine's " + method + "() takes no argument " + name);
            }
        }
        Object v = m.apply(new Faker(new Random(digest)), args);
        if (v == null) {
            return null;
        }
        if (v instanceof LocalDateTime t) {
            return t.format(STAMP);
        }
        return String.valueOf(v);
    }

    static Iterable<String> names()
    {
        return METHODS.keySet();
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

    private interface Gen
    {
        Object make(Ctx c);
    }

    private static Map<String, BiFunction<Faker, JsonNode, Object>> methods()
    {
        Map<String, Gen> g = new TreeMap<>();
        // -- people
        g.put("name", c -> c.f.name().fullName());
        g.put("name_female", c -> c.f.name().femaleFirstName() + " " + c.f.name().lastName());
        g.put("name_male", c -> c.f.name().malefirstName() + " " + c.f.name().lastName());
        g.put("name_nonbinary", c -> c.f.name().firstName() + " " + c.f.name().lastName());
        g.put("first_name", c -> c.f.name().firstName());
        g.put("first_name_female", c -> c.f.name().femaleFirstName());
        g.put("first_name_male", c -> c.f.name().malefirstName());
        g.put("first_name_nonbinary", c -> c.f.name().firstName());
        g.put("last_name", c -> c.f.name().lastName());
        g.put("last_name_female", c -> c.f.name().lastName());
        g.put("last_name_male", c -> c.f.name().lastName());
        g.put("last_name_nonbinary", c -> c.f.name().lastName());
        g.put("prefix", c -> c.f.name().prefix());
        g.put("prefix_female", c -> c.pick("Mrs.", "Ms.", "Miss", "Dr."));
        g.put("prefix_male", c -> c.pick("Mr.", "Dr."));
        g.put("prefix_nonbinary", c -> c.pick("Mx.", "Dr."));
        g.put("suffix", c -> c.f.name().suffix());
        g.put("suffix_female", c -> c.pick("MD", "DDS", "PhD", "DVM"));
        g.put("suffix_male", c -> c.pick("Jr.", "Sr.", "I", "II", "III", "IV", "V", "MD", "DDS", "PhD", "DVM"));
        g.put("suffix_nonbinary", c -> c.pick("MD", "DDS", "PhD", "DVM"));
        g.put("job", c -> c.f.job().title());
        g.put("job_female", c -> c.f.job().title());
        g.put("job_male", c -> c.f.job().title());
        g.put("user_name", c -> c.f.internet().username());
        g.put("password", c -> password(c));
        // -- contact
        g.put("email", c -> c.f.internet().emailAddress());
        g.put("ascii_email", c -> c.f.internet().emailAddress());
        g.put("free_email", c -> c.f.internet().username() + "@" + c.pick("gmail.com", "yahoo.com", "hotmail.com"));
        g.put("ascii_free_email", c -> c.f.internet().username() + "@" + c.pick("gmail.com", "yahoo.com", "hotmail.com"));
        g.put("free_email_domain", c -> c.pick("gmail.com", "yahoo.com", "hotmail.com"));
        g.put("safe_email", c -> c.f.internet().username() + "@" + c.pick("example.com", "example.net", "example.org"));
        g.put("ascii_safe_email", c -> c.f.internet().username() + "@" + c.pick("example.com", "example.net", "example.org"));
        g.put("company_email", c -> c.f.internet().username() + "@" + companyDomain(c));
        g.put("ascii_company_email", c -> c.f.internet().username() + "@" + companyDomain(c));
        g.put("phone_number", c -> c.f.phoneNumber().phoneNumber());
        g.put("basic_phone_number", c -> c.f.phoneNumber().phoneNumberNational());
        g.put("msisdn", c -> c.digits(13));
        g.put("country_calling_code", c -> "+" + (1 + c.r.nextInt(998)));
        // -- address
        g.put("address", c -> c.f.address().fullAddress());
        g.put("street_address", c -> c.f.address().streetAddress());
        g.put("street_name", c -> c.f.address().streetName());
        g.put("street_suffix", c -> c.f.address().streetSuffix());
        g.put("secondary_address", c -> c.f.address().secondaryAddress());
        g.put("building_number", c -> c.f.address().buildingNumber());
        g.put("city", c -> c.f.address().city());
        g.put("city_prefix", c -> c.f.address().cityPrefix());
        g.put("city_suffix", c -> c.f.address().citySuffix());
        g.put("state", c -> c.f.address().state());
        g.put("state_abbr", c -> c.f.address().stateAbbr());
        g.put("administrative_unit", c -> c.f.address().state());
        g.put("country", c -> c.f.address().country());
        g.put("country_code", c -> c.f.address().countryCode());
        g.put("current_country", c -> "United States");
        g.put("current_country_code", c -> "US");
        for (String zip : List.of("postcode", "postalcode", "zipcode", "postcode_in_state", "postalcode_in_state", "zipcode_in_state")) {
            g.put(zip, c -> c.f.address().zipCode());
        }
        g.put("postalcode_plus4", c -> c.f.address().zipCodePlus4());
        g.put("zipcode_plus4", c -> c.f.address().zipCodePlus4());
        g.put("military_apo", c -> "PSC " + c.digits(4) + ", Box " + c.digits(4));
        g.put("military_dpo", c -> "Unit " + c.digits(4) + " Box " + c.digits(4));
        g.put("military_ship", c -> c.pick("USS", "USNS", "USNV", "USCGC"));
        g.put("military_state", c -> c.pick("AA", "AE", "AP"));
        g.put("timezone", c -> c.f.address().timeZone());
        g.put("coordinate", c -> String.format(Locale.ROOT, "%.6f", c.r.nextDouble(-180, 180)));
        g.put("latitude", c -> String.format(Locale.ROOT, "%.6f", c.r.nextDouble(-90, 90)));
        g.put("longitude", c -> String.format(Locale.ROOT, "%.6f", c.r.nextDouble(-180, 180)));
        // -- companies, finance
        g.put("company", c -> c.f.company().name());
        g.put("company_suffix", c -> c.f.company().suffix());
        g.put("catch_phrase", c -> c.f.company().catchPhrase());
        g.put("bs", c -> c.f.company().bs());
        g.put("bank", c -> c.f.name().lastName() + " Bank");
        g.put("bank_country", c -> c.f.address().countryCode());
        g.put("aba", c -> c.f.finance().usRoutingNumber());
        g.put("bban", c -> c.letters(4, true) + c.digits(14));
        g.put("iban", c -> c.f.finance().iban());
        g.put("swift", c -> c.f.finance().bic());
        g.put("swift8", c -> c.f.finance().bic().substring(0, 8));
        g.put("swift11", c -> (c.f.finance().bic() + "XXX").substring(0, 11));
        g.put("ein", c -> c.digits(2) + "-" + c.digits(7));
        g.put("ssn", c -> c.f.idNumber().ssnValid());
        g.put("invalid_ssn", c -> "9" + c.digits(2) + "-" + c.digits(2) + "-" + c.digits(4));
        g.put("itin", c -> "9" + c.digits(2) + "-7" + c.digits(1) + "-" + c.digits(4));
        g.put("credit_card_number", c -> c.f.business().creditCardNumber());
        g.put("credit_card_provider", c -> c.f.business().creditCardType());
        g.put("credit_card_expire", c -> c.f.business().creditCardExpiry());
        g.put("credit_card_security_code", c -> c.f.business().securityCode());
        g.put("credit_card_full", c -> c.f.business().creditCardType() + "\n" + c.f.name().fullName() + "\n"
                + c.f.business().creditCardNumber() + " " + c.f.business().creditCardExpiry() + "\nCVV: " + c.f.business().securityCode() + "\n");
        g.put("currency_code", c -> c.f.money().currencyCode());
        g.put("currency_name", c -> c.f.currency().name());
        g.put("currency_symbol", c -> c.f.money().currencySymbol());
        g.put("cryptocurrency_code", c -> c.pick("BTC", "ETH", "LTC", "XRP", "ADA", "DOGE", "SOL", "DOT"));
        g.put("cryptocurrency_name", c -> c.pick("Bitcoin", "Ethereum", "Litecoin", "Ripple", "Cardano", "Dogecoin", "Solana", "Polkadot"));
        g.put("pricetag", c -> String.format(Locale.US, "$%,.2f", c.r.nextDouble(1, 10000)));
        // -- codes and identifiers
        g.put("ean", c -> c.f.code().ean13());
        g.put("ean13", c -> c.f.code().ean13());
        g.put("ean8", c -> c.f.code().ean8());
        g.put("localized_ean", c -> c.f.code().ean13());
        g.put("localized_ean13", c -> c.f.code().ean13());
        g.put("localized_ean8", c -> c.f.code().ean8());
        g.put("upc_a", c -> c.f.barcode().gtin12());
        g.put("upc_e", c -> "0" + c.digits(7));
        g.put("isbn10", c -> c.f.code().isbn10());
        g.put("isbn13", c -> c.f.code().isbn13());
        g.put("sbn9", c -> c.digits(3) + "-" + c.digits(5) + "-" + c.digits(1));
        g.put("doi", c -> "10." + c.digits(4) + "/" + c.letters(4, false) + c.digits(4));
        g.put("license_plate", c -> c.letters(3, true) + "-" + c.digits(4));
        g.put("vin", c -> c.f.vehicle().vin());
        g.put("passport_number", c -> c.f.passport().valid());
        g.put("passport_gender", c -> c.pick("M", "F", "X"));
        g.put("passport_full", c -> c.f.name().fullName() + "\n" + c.pick("M", "F", "X") + "\n" + c.f.passport().valid() + "\n");
        g.put("nic_handle", c -> c.letters(2 + c.r.nextInt(3), true) + c.digits(1 + c.r.nextInt(4)) + "-" + c.text("suffix", "FAKE"));
        g.put("ripe_id", c -> "ORG-" + c.letters(2 + c.r.nextInt(3), true) + c.digits(1 + c.r.nextInt(5)) + "-RIPE");
        g.put("iana_id", c -> String.valueOf(1 + c.r.nextInt(8888888)));
        g.put("uuid4", c -> c.f.internet().uuidv4());
        g.put("uuid7", c -> c.f.internet().uuidv7());
        g.put("uuid1", c -> new UUID(c.r.nextLong() & 0xFFFFFFFFFFFF0FFFL | 0x1000L, c.r.nextLong() & 0x3FFFFFFFFFFFFFFFL | 0x8000000000000000L).toString());
        g.put("md5", c -> hex(c, 32));
        g.put("sha1", c -> hex(c, 40));
        g.put("sha256", c -> hex(c, 64));
        // -- internet
        g.put("domain_name", c -> c.f.internet().domainName());
        g.put("domain_word", c -> c.f.internet().domainWord());
        g.put("safe_domain_name", c -> c.pick("example.com", "example.net", "example.org"));
        g.put("dga", c -> c.letters(10 + c.r.nextInt(20), false) + "." + c.pick("com", "net", "org", "info"));
        g.put("tld", c -> c.pick("com", "net", "org", "info", "biz"));
        g.put("hostname", c -> c.pick("web", "db", "srv", "lt", "email", "laptop", "desktop") + "-" + c.digits(2) + "." + c.f.internet().domainName());
        g.put("url", c -> "https://" + c.f.internet().domainName() + "/");
        g.put("uri", c -> "https://" + c.f.internet().domainName() + "/" + c.f.lorem().word() + "/" + uriPage(c) + uriExtension(c));
        g.put("uri_page", FakeMethods::uriPage);
        g.put("uri_path", c -> c.f.lorem().word() + "/" + c.f.lorem().word());
        g.put("uri_extension", FakeMethods::uriExtension);
        g.put("image_url", c -> "https://picsum.photos/" + (100 + c.r.nextInt(900)) + "/" + (100 + c.r.nextInt(900)));
        g.put("slug", c -> c.f.lorem().word() + "-" + c.f.lorem().word() + "-" + c.f.lorem().word());
        g.put("ipv4", c -> c.f.internet().ipV4Address());
        g.put("ipv4_private", c -> c.f.internet().privateIpV4Address());
        g.put("ipv4_public", c -> c.f.internet().publicIpV4Address());
        g.put("ipv4_network_class", c -> c.pick("a", "b", "c"));
        g.put("ipv6", c -> c.f.internet().ipV6Address());
        g.put("mac_address", c -> c.f.internet().macAddress());
        g.put("port_number", c -> c.r.nextInt(65536));
        g.put("http_method", c -> c.f.internet().httpMethod());
        g.put("http_status_code", c -> Integer.valueOf(c.pick("200", "201", "204", "301", "302", "304", "400", "401", "403", "404", "409", "500", "502", "503")));
        g.put("user_agent", c -> c.f.internet().userAgent());
        for (String browser : List.of("chrome", "firefox", "safari", "opera", "internet_explorer")) {
            g.put(browser, c -> c.f.internet().userAgent());
        }
        g.put("android_platform_token", c -> "Android " + (5 + c.r.nextInt(10)));
        g.put("ios_platform_token", c -> "iPhone; CPU iPhone OS " + (10 + c.r.nextInt(8)) + "_" + c.r.nextInt(5) + " like Mac OS X");
        g.put("linux_platform_token", c -> "X11; Linux " + c.pick("i686", "x86_64"));
        g.put("linux_processor", c -> c.pick("i686", "x86_64"));
        g.put("mac_platform_token", c -> "Macintosh; " + c.pick("U; Intel", "U; PPC", "Intel") + " Mac OS X 10_" + (5 + c.r.nextInt(12)) + "_" + c.r.nextInt(9));
        g.put("mac_processor", c -> c.pick("U; Intel", "U; PPC", "Intel"));
        g.put("windows_platform_token", c -> c.pick("Windows NT 10.0", "Windows NT 6.3", "Windows NT 6.2", "Windows NT 6.1", "Windows NT 6.0"));
        g.put("unix_device", c -> "/dev/" + c.pick("sd", "vd", "xvd") + c.letters(1, false));
        g.put("unix_partition", c -> "/dev/" + c.pick("sd", "vd", "xvd") + c.letters(1, false) + (1 + c.r.nextInt(9)));
        g.put("file_name", c -> c.f.file().fileName());
        g.put("file_extension", c -> c.f.file().extension());
        g.put("file_path", c -> "/" + c.f.lorem().word() + "/" + c.f.lorem().word() + "." + c.f.file().extension());
        g.put("mime_type", c -> c.f.file().mimeType());
        // -- language, locale, colour
        g.put("language_code", c -> c.f.languageCode().iso639());
        g.put("language_name", c -> c.f.nation().language());
        g.put("locale", c -> c.f.locality().localeString().replace('-', '_'));
        g.put("color", c -> c.f.color().hex());
        g.put("color_name", c -> c.f.color().name());
        g.put("safe_color_name", c -> c.pick("black", "maroon", "green", "navy", "olive", "purple", "teal", "lime", "blue", "silver", "gray", "yellow", "fuchsia", "aqua", "white"));
        g.put("hex_color", c -> hexColor(c));
        g.put("safe_hex_color", c -> hexColor(c));
        g.put("rgb_color", c -> c.r.nextInt(256) + "," + c.r.nextInt(256) + "," + c.r.nextInt(256));
        g.put("rgb_css_color", c -> "rgb(" + c.r.nextInt(256) + "," + c.r.nextInt(256) + "," + c.r.nextInt(256) + ")");
        g.put("emoji", c -> c.pick("😀", "😂", "😍", "👍", "🎉", "🔥", "🌟", "🍕", "🚀", "🐶"));
        // -- text
        g.put("word", c -> c.f.lorem().word());
        g.put("sentence", c -> c.f.lorem().sentence((int) c.num("nb_words", 6)));
        g.put("paragraph", c -> c.f.lorem().paragraph((int) c.num("nb_sentences", 3)));
        g.put("text", c -> truncate(c.f.lorem().paragraph(), (int) c.num("max_nb_chars", 200)));
        g.put("pystr", c -> {
            int max = (int) c.num("max_chars", 20);
            int len = c.a.has("min_chars") ? (int) c.num("min_chars", 0) + c.r.nextInt(max - (int) c.num("min_chars", 0) + 1) : max;
            StringBuilder sb = new StringBuilder(c.text("prefix", ""));
            for (int i = 0; i < len; i++) {
                sb.append(ASCII_LETTERS.charAt(c.r.nextInt(ASCII_LETTERS.length())));
            }
            return sb.append(c.text("suffix", "")).toString();
        });
        g.put("words", c -> String.join(" ", c.f.lorem().words((int) c.num("nb", 3))));
        g.put("sentences", c -> String.join(" ", c.f.lorem().sentences((int) c.num("nb", 3))));
        g.put("paragraphs", c -> String.join("\n", c.f.lorem().paragraphs((int) c.num("nb", 3))));
        g.put("texts", c -> {
            java.util.List<String> out = new java.util.ArrayList<>();
            for (int i = 0; i < c.num("nb_texts", 3); i++) {
                out.add(truncate(c.f.lorem().paragraph(), (int) c.num("max_nb_chars", 200)));
            }
            return String.join("\n", out);
        });
        g.put("get_words_list", c -> String.join(" ", c.f.lorem().words(100)));
        g.put("random_letters", c -> {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < c.num("length", 16); i++) {
                sb.append(ASCII_LETTERS.charAt(c.r.nextInt(ASCII_LETTERS.length())));
            }
            return sb.toString();
        });
        g.put("random_choices", FakeMethods::choices);
        g.put("random_elements", c -> c.flag("unique", false) ? sample(c) : choices(c));
        g.put("random_sample", FakeMethods::sample);
        g.put("nic_handles", c -> {
            java.util.List<String> out = new java.util.ArrayList<>();
            for (int i = 0; i < c.num("count", 1); i++) {
                out.add(c.letters(2 + c.r.nextInt(3), true) + c.digits(1 + c.r.nextInt(4)) + "-" + c.f.letterify(c.text("suffix", "????")).toUpperCase(Locale.ROOT));
            }
            return String.join(" ", out);
        });
        g.put("currency", c -> c.f.money().currencyCode());
        g.put("cryptocurrency", c -> c.pick("BTC", "ETH", "LTC", "XRP", "ADA", "DOGE", "SOL", "DOT"));
        g.put("pystr_format", c -> c.f.bothify("?#-###") + c.r.nextInt(10000) + c.letters(1, true));
        g.put("bothify", c -> c.fill(c.text("text", "## ??"), c.text("letters", ASCII_LETTERS)));
        g.put("numerify", c -> c.fill(c.text("text", "###").replace('?', '\u0000'), "a").replace('\u0000', '?'));
        g.put("lexify", c -> c.fill(c.text("text", "????").replace('#', '\u0000'), c.text("letters", ASCII_LETTERS)).replace('\u0000', '#'));
        g.put("hexify", c -> hexify(c, c.text("text", "^^^^"), c.a.path("upper").asBoolean(false)));
        g.put("random_letter", c -> c.letters(1, c.r.nextBoolean()));
        g.put("random_lowercase_letter", c -> c.letters(1, false));
        g.put("random_uppercase_letter", c -> c.letters(1, true));
        g.put("random_element", c -> {
            List<String> e = c.elements();
            return e.get(c.r.nextInt(e.size()));
        });
        g.put("random_digit_or_empty", c -> c.r.nextBoolean() ? String.valueOf(c.r.nextInt(10)) : "");
        g.put("random_digit_not_null_or_empty", c -> c.r.nextBoolean() ? String.valueOf(1 + c.r.nextInt(9)) : "");
        g.put("csv", c -> delimited(c, ","));
        g.put("tsv", c -> delimited(c, "\t"));
        g.put("psv", c -> delimited(c, "|"));
        g.put("dsv", c -> delimited(c, ","));
        g.put("fixed_width", FakeMethods::fixedWidth);
        g.put("json", FakeMethods::json);
        // -- numbers and booleans
        g.put("boolean", c -> c.r.nextInt(100) < c.num("chance_of_getting_true", 50));
        g.put("pybool", c -> c.r.nextInt(100) < c.num("truth_probability", 50));
        g.put("null_boolean", c -> switch (c.r.nextInt(3)) {
            case 0 -> null;
            case 1 -> Boolean.TRUE;
            default -> Boolean.FALSE;
        });
        g.put("pyint", c -> stepped(c, c.num("min_value", 0), c.num("max_value", 9999), c.num("step", 1)));
        g.put("random_int", c -> stepped(c, c.num("min", 0), c.num("max", 9999), c.num("step", 1)));
        g.put("random_number", c -> {
            int digits = c.a.has("digits") ? (int) c.num("digits", 9) : 1 + c.r.nextInt(9);
            long top = (long) Math.pow(10, digits);
            return c.flag("fix_len", false) ? top / 10 + c.r.nextLong(top - top / 10) : c.r.nextLong(top);
        });
        g.put("random_digit", c -> c.r.nextInt(10));
        g.put("random_digit_not_null", c -> 1 + c.r.nextInt(9));
        g.put("random_digit_above_two", c -> 2 + c.r.nextInt(8));
        g.put("randomize_nb_elements", c -> 6 + c.r.nextInt(9));
        g.put("pyfloat", FakeMethods::pyfloat);
        g.put("pydecimal", FakeMethods::pyfloat);
        // -- dates and times
        LocalDate epoch = LocalDate.of(1970, 1, 1);
        LocalDateTime epochTs = epoch.atStartOfDay();
        g.put("date", c -> c.between(epochTs, c.moment("end_datetime", "now")).format(strftime(c.text("pattern", "%Y-%m-%d"))));
        g.put("time", c -> c.between(epochTs, c.moment("end_datetime", "now")).toLocalTime().format(strftime(c.text("pattern", "%H:%M:%S"))));
        g.put("date_object", c -> c.between(epochTs, c.moment("end_datetime", "now")).toLocalDate());
        g.put("date_between", c -> c.between(c.moment("start_date", "-30y"), c.moment("end_date", "today")).toLocalDate());
        g.put("date_time_between", c -> c.between(c.moment("start_date", "-30y"), c.moment("end_date", "now")));
        g.put("date_between_dates", c -> c.between(c.moment("date_start", "now"), c.moment("date_end", "now")).toLocalDate());
        g.put("date_time_between_dates", c -> c.between(c.moment("datetime_start", "now"), c.moment("datetime_end", "now")));
        g.put("date_of_birth", c -> c.date(TODAY.minusYears(c.num("maximum_age", 115) + 1).plusDays(1), TODAY.minusYears(c.num("minimum_age", 0))));
        g.put("passport_dob", c -> c.date(TODAY.minusYears(115), TODAY));
        g.put("future_date", c -> c.between(NOW.plusDays(1), c.moment("end_date", "+30d")).toLocalDate());
        g.put("future_datetime", c -> c.between(NOW.plusSeconds(1), c.moment("end_date", "+30d")));
        g.put("past_date", c -> c.between(c.moment("start_date", "-30d"), NOW.minusDays(1)).toLocalDate());
        g.put("past_datetime", c -> c.between(c.moment("start_date", "-30d"), NOW.minusSeconds(1)));
        thisPeriod(g, "century", LocalDate.of(2000, 1, 1), LocalDate.of(2100, 1, 1));
        thisPeriod(g, "decade", LocalDate.of(2020, 1, 1), LocalDate.of(2030, 1, 1));
        thisPeriod(g, "year", TODAY.withDayOfYear(1), TODAY.withDayOfYear(1).plusYears(1));
        thisPeriod(g, "month", TODAY.withDayOfMonth(1), TODAY.withDayOfMonth(1).plusMonths(1));
        g.put("date_time", c -> c.between(epochTs, c.moment("end_datetime", "now")));
        g.put("date_time_ad", c -> c.between(c.moment("start_datetime", "0001-01-01"), c.moment("end_datetime", "now")));
        g.put("iso8601", c -> c.between(epochTs, c.moment("end_datetime", "now")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'" + c.text("sep", "T").replace("'", "''") + "'HH:mm:ss")));
        g.put("unix_time", c -> {
            LocalDateTime t = c.between(c.moment("start_datetime", epochTs.toString()), c.moment("end_datetime", "now"));
            return String.valueOf(t.toEpochSecond(java.time.ZoneOffset.UTC));
        });
        g.put("year", c -> String.valueOf(1970 + c.r.nextInt(TODAY.getYear() - 1970 + 1)));
        g.put("month", c -> String.format(Locale.ROOT, "%02d", 1 + c.r.nextInt(12)));
        g.put("month_name", c -> java.time.Month.of(1 + c.r.nextInt(12)).getDisplayName(TextStyle.FULL, Locale.US));
        g.put("day_of_month", c -> String.format(Locale.ROOT, "%02d", 1 + c.r.nextInt(31)));
        g.put("day_of_week", c -> java.time.DayOfWeek.of(1 + c.r.nextInt(7)).getDisplayName(TextStyle.FULL, Locale.US));
        g.put("am_pm", c -> c.pick("AM", "PM"));
        g.put("century", c -> c.pick("I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII", "XIII", "XIV", "XV", "XVI", "XVII", "XVIII", "XIX", "XX", "XXI"));

        Map<String, BiFunction<Faker, JsonNode, Object>> out = new TreeMap<>();
        g.forEach((name, gen) -> out.put(name, (faker, args) -> gen.make(new Ctx(faker, faker.random(), args))));
        return Map.copyOf(out);
    }

    private static void thisPeriod(Map<String, Gen> g, String period, LocalDate start, LocalDate end)
    {
        g.put("date_this_" + period, c -> {
            boolean before = c.flag("before_today", true);
            boolean after = c.flag("after_today", false);
            return c.date(before ? start : TODAY, after ? end.minusDays(1) : TODAY);
        });
        g.put("date_time_this_" + period, c -> {
            boolean before = c.flag("before_now", true);
            boolean after = c.flag("after_now", false);
            return c.between(before ? start.atStartOfDay() : NOW, after ? end.atStartOfDay().minusSeconds(1) : NOW);
        });
    }

    private static long stepped(Ctx c, long min, long max, long step)
    {
        long steps = (max - min) / Math.max(1, step);
        return min + Math.max(1, step) * c.r.nextLong(steps + 1);
    }

    private static String pyfloat(Ctx c)
    {
        int right = c.a.has("right_digits") ? (int) c.num("right_digits", 6) : c.r.nextInt(7);
        double value;
        if (c.a.has("min_value") || c.a.has("max_value")) {
            double lo = c.a.has("min_value") ? c.a.get("min_value").asDouble() : (c.flag("positive", false) ? 0 : -1e6);
            double hi = c.a.has("max_value") ? c.a.get("max_value").asDouble() : 1e6;
            value = c.r.nextDouble(lo, hi);
        }
        else {
            int left = c.a.has("left_digits") ? (int) c.num("left_digits", 6) : 1 + c.r.nextInt(6);
            value = c.r.nextDouble(0, Math.pow(10, left));
            if (!c.flag("positive", false) && c.r.nextBoolean()) {
                value = -value;
            }
        }
        return new java.math.BigDecimal(value).setScale(right, java.math.RoundingMode.HALF_UP).toPlainString();
    }

    private static String password(Ctx c)
    {
        StringBuilder pool = new StringBuilder();
        if (c.flag("lower_case", true)) {
            pool.append("abcdefghijklmnopqrstuvwxyz");
        }
        if (c.flag("upper_case", true)) {
            pool.append("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
        }
        if (c.flag("digits", true)) {
            pool.append("0123456789");
        }
        if (c.flag("special_chars", true)) {
            pool.append("!@#$%^&*()_+");
        }
        if (pool.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "password() needs one kind of character");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < c.num("length", 10); i++) {
            sb.append(pool.charAt(c.r.nextInt(pool.length())));
        }
        return sb.toString();
    }

    private static String choices(Ctx c)
    {
        java.util.List<String> e = c.elements();
        long n = c.a.has("length") ? c.num("length", 1) : 1 + c.r.nextInt(e.size());
        java.util.List<String> out = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(e.get(c.r.nextInt(e.size())));
        }
        return String.join(" ", out);
    }

    private static String sample(Ctx c)
    {
        java.util.List<String> e = new java.util.ArrayList<>(c.elements());
        long n = Math.min(e.size(), c.a.has("length") ? c.num("length", 1) : 1 + c.r.nextInt(e.size()));
        java.util.List<String> out = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(e.remove(c.r.nextInt(e.size())));
        }
        return String.join(" ", out);
    }

    private static String companyDomain(Ctx c)
    {
        return c.f.name().lastName().toLowerCase(Locale.ROOT).replaceAll("[^a-z]", "") + "." + c.pick("com", "net", "org", "biz", "info");
    }

    private static String uriPage(Ctx c)
    {
        return c.pick("index", "home", "search", "main", "post", "homepage", "category", "register", "login", "faq", "about", "terms", "privacy", "author");
    }

    private static String uriExtension(Ctx c)
    {
        return c.pick(".html", ".html", ".html", ".htm", ".htm", ".php", ".php", ".jsp", ".asp");
    }

    private static String hex(Ctx c, int n)
    {
        return hexify(c, "^".repeat(n), false);
    }

    private static String hexColor(Ctx c)
    {
        return "#" + hex(c, 6);
    }

    private static String hexify(Ctx c, String text, boolean upper)
    {
        String alphabet = upper ? "0123456789ABCDEF" : "0123456789abcdef";
        StringBuilder sb = new StringBuilder();
        for (char ch : text.toCharArray()) {
            sb.append(ch == '^' ? alphabet.charAt(c.r.nextInt(16)) : ch);
        }
        return sb.toString();
    }

    private static String delimited(Ctx c, String sep)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append('"').append(c.f.name().fullName()).append('"').append(sep)
                    .append('"').append(c.f.address().fullAddress()).append('"').append("\r\n");
        }
        return sb.toString();
    }

    private static String fixedWidth(Ctx c)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append(String.format(Locale.ROOT, "%-20s%-3s%n", truncate(c.f.name().fullName(), 20), c.f.address().stateAbbr()));
        }
        return sb.toString();
    }

    private static String json(Ctx c)
    {
        ArrayNode rows = JSON.createArrayNode();
        for (int i = 0; i < 10; i++) {
            ObjectNode row = rows.addObject();
            row.put("name", c.f.name().fullName());
            row.put("residency", c.f.address().fullAddress());
        }
        return rows.toString();
    }

    private static String truncate(String s, int n)
    {
        return s.length() <= n ? s : s.substring(0, n);
    }
}
