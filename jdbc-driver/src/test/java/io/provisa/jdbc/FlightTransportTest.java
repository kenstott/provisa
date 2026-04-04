package io.provisa.jdbc;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FlightTransportTest {

    @Test
    void buildTicket_includesQueryAndRole() {
        byte[] ticket = FlightTransport.buildTicket(
            "{ orders { id } }", "analyst", null
        );
        JsonObject json = JsonParser.parseString(new String(ticket)).getAsJsonObject();

        assertEquals("{ orders { id } }", json.get("query").getAsString());
        assertEquals("analyst", json.get("role").getAsString());
        assertFalse(json.has("variables"));
    }

    @Test
    void buildTicket_includesVariables() {
        byte[] ticket = FlightTransport.buildTicket(
            "query Q($r: String) { orders(where: {region: {eq: $r}}) { id } }",
            "admin",
            Map.of("r", "us-east")
        );
        JsonObject json = JsonParser.parseString(new String(ticket)).getAsJsonObject();

        assertTrue(json.has("variables"));
        assertEquals("us-east", json.getAsJsonObject("variables").get("r").getAsString());
    }

    @Test
    void buildTicket_emptyVariablesOmitted() {
        byte[] ticket = FlightTransport.buildTicket("{ orders { id } }", "admin", Map.of());
        JsonObject json = JsonParser.parseString(new String(ticket)).getAsJsonObject();

        assertFalse(json.has("variables"));
    }

    @Test
    void tryConnect_returnsNullOnUnreachable() {
        // No Flight server running on this port
        FlightTransport transport = FlightTransport.tryConnect("localhost", 19999, "admin");
        assertNull(transport);
    }

    @Test
    void tryConnect_returnsNullOnBadHost() {
        FlightTransport transport = FlightTransport.tryConnect("nonexistent.invalid", 8815, "admin");
        assertNull(transport);
    }
}
