package com.crushdb.model;

import com.crushdb.model.document.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DocumentTest {

    private final String TO_STRING_DOCUMENT = "{\"_id\": 123456789, \"vehicle_make\": \"Subaru\", \"vehicle_model\": \"Forester\", \"vehicle_year\": 2017, \"vehicle_type\": \"automobile\", \"vehicle_body_style\": \"SUV\", \"vehicle_price\": 28500.99, \"hasHeating\": true}";


    /**
     * <p>
     * {
     *     "_id": 123456789,
     *     "vehicle_make": "Subaru",
     *     "vehicle_model": "Forester",
     *     "vehicle_year": 2017,
     *     "vehicle_type": "automobile",
     *     "vehicle_body_style": "SUV",
     *     "vehicle_price": 28500.99,
     *     "hasHeating", true
     * }
     * </p>
     */
    @Test
    void testToString() {
        Document document = new Document(123456789L);
        document.put("vehicle_make", "Subaru");
        document.put("vehicle_model", "Forester");
        document.put("vehicle_year", 2017);
        document.put("vehicle_type", "automobile");
        document.put("vehicle_body_style", "SUV");
        document.put("vehicle_price", 28500.99);
        document.put("hasHeating", true);

        assertAll(
                () -> assertEquals(TO_STRING_DOCUMENT, document.toString()),
                () -> assertEquals(123456789, document.getDocumentId()),
                () -> assertEquals(123456789L , document.getDocumentId()),
                () -> assertEquals("Subaru", document.getString("vehicle_make")),
                () -> assertEquals("Forester", document.getString("vehicle_model")),
                () -> assertEquals(2017, document.getInt("vehicle_year")),
                () -> assertEquals("automobile", document.getString("vehicle_type")),
                () -> assertEquals("SUV", document.getString("vehicle_body_style")),
                () -> assertEquals(28500.99, document.getDouble("vehicle_price")),
                () -> assertTrue(document.getBoolean("hasHeating"))
        );
        document.prettyPrint();
    }

    @Test
    void toBytes() {
    }

    @Test
    void fromBytes() {
    }

    @Test
    void getFields() {
    }

    @Test
    void put() {
    }

    @Test
    void getString() {
    }

    @Test
    void getInt() {
    }

    @Test
    void getLong() {
    }

    @Test
    void getFloat() {
    }

    @Test
    void getDouble() {
    }

    @Test
    void getBoolean() {
    }

    @Test
    void getDocumentId() {
    }

    @Test
    void getPageId() {
    }

    @Test
    void getDecompressedSize() {
    }

    @Test
    void getCompressedSize() {
    }
}