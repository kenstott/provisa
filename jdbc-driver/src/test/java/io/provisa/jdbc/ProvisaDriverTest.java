package io.provisa.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ProvisaDriverTest {

    private final ProvisaDriver driver = new ProvisaDriver();

    @Test
    void acceptsValidUrl() {
        assertTrue(driver.acceptsURL("jdbc:provisa://localhost:8001"));
    }

    @Test
    void rejectsNullUrl() {
        assertFalse(driver.acceptsURL(null));
    }

    @Test
    void rejectsOtherDriverUrl() {
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432/test"));
    }

    @Test
    void acceptsUrlWithMode() {
        assertTrue(driver.acceptsURL("jdbc:provisa://localhost:8001?mode=catalog"));
    }

    @Test
    void propertyInfoIncludesMode() {
        DriverPropertyInfo[] props = driver.getPropertyInfo("jdbc:provisa://localhost:8001", new Properties());
        assertEquals(3, props.length);
        assertEquals("mode", props[2].name);
        assertArrayEquals(new String[]{"approved", "catalog"}, props[2].choices);
    }

    @Test
    void versionNumbers() {
        assertEquals(0, driver.getMajorVersion());
        assertEquals(1, driver.getMinorVersion());
        assertFalse(driver.jdbcCompliant());
    }
}
