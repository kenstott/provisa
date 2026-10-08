package io.provisa.jdbc;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FlightTransportTest {

    private static JsonObject ticket(String token, String role, String kmsKey, Map<String, Object> vars) {
        byte[] bytes = FlightTransport.buildTicket("SELECT 1", token, role, kmsKey, vars);
        return JsonParser.parseString(new String(bytes, java.nio.charset.StandardCharsets.UTF_8))
            .getAsJsonObject();
    }

    @Test
    void theTicketCarriesTheSessionTokenInTheServersForm() {
        JsonObject json = ticket("tok-1", null, null, null);
        assertEquals("SELECT 1", json.get("query").getAsString());
        assertEquals("tok-1", json.get("token").getAsString());
    }

    @Test
    void aRoleIsSentOnlyWhenTheConnectionRequestsOne() {
        assertFalse(ticket("tok-1", null, null, null).has("role"));
        assertEquals("analyst,auditor", ticket("tok-1", "analyst,auditor", null, null).get("role").getAsString());
    }

    @Test
    void aConnectionWithNoTokenSendsNone() {
        JsonObject json = ticket(null, "analyst", null, null);
        assertFalse(json.has("token"));
        assertEquals("analyst", json.get("role").getAsString());
    }

    @Test
    void theClientSideDecryptionKeyIsNamedWhenConfigured() {
        assertFalse(ticket("tok-1", null, null, null).has("kms_key"));
        assertEquals("arn:key", ticket("tok-1", null, "arn:key", null).get("kms_key").getAsString());
    }

    @Test
    void variablesAreSentWhenThereAreAny() {
        assertFalse(ticket("tok-1", null, null, Map.of()).has("variables"));
        assertEquals("us-east",
            ticket("tok-1", null, null, Map.of("r", "us-east")).getAsJsonObject("variables").get("r").getAsString());
    }

    @Test
    void anUnreachablePortIsNoTransportNotAnError() throws java.sql.SQLException {
        // Nothing listens here: the one case in which queries go over HTTP.
        assertNull(FlightTransport.connect("127.0.0.1", 19999));
    }

    @Test
    void aHostThatDoesNotResolveIsUnreachableToo() throws java.sql.SQLException {
        assertNull(FlightTransport.connect("nonexistent.invalid", 8815));
    }

    @Test
    void theFlightPortFollowsTheHttpPort() {
        assertEquals(8815, FlightTransport.deriveFlightPort(8001));
        assertEquals(9815, FlightTransport.deriveFlightPort(9001));
    }
}
