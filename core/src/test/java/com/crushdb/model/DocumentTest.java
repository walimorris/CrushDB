package com.crushdb.model;

import com.crushdb.model.document.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DocumentTest {

    private final String TO_STRING_DOCUMENT = "{\"_id\": 123456789, \"vehicleMake\": \"Subaru\", \"vehicleModel\": \"Forester\", \"vehicleYear\": 2017, \"vehicleType\": \"automobile\", \"vehicleBodyStyle\": \"SUV\", \"vehiclePrice\": 28500.99, \"hasHeating\": true}";


    /**
     * <p>
     * {
     *     "_id": 123456789,
     *     "vehicleMake": "Subaru",
     *     "vehicleModel": "Forester",
     *     "vehicleYear": 2017,
     *     "vehicleType": "automobile",
     *     "vehicleBodyStyle": "SUV",
     *     "vehiclePrice": 28500.99,
     *     "hasHeating", true
     * }
     * </p>
     */
    @Test
    void testToString() {
        Document document = new Document(123456789L);
        document.put("vehicleMake", "Subaru");
        document.put("vehicleModel", "Forester");
        document.put("vehicleYear", 2017);
        document.put("vehicleType", "automobile");
        document.put("vehicleBodyStyle", "SUV");
        document.put("vehiclePrice", 28500.99);
        document.put("hasHeating", true);

        assertAll(
                () -> assertEquals(TO_STRING_DOCUMENT, document.toString()),
                () -> assertEquals(123456789, document.getDocumentId()),
                () -> assertEquals(123456789L , document.getDocumentId()),
                () -> assertEquals("Subaru", document.getString("vehicleMake")),
                () -> assertEquals("Forester", document.getString("vehicleModel")),
                () -> assertEquals(2017, document.getInt("vehicleYear")),
                () -> assertEquals("automobile", document.getString("vehicleType")),
                () -> assertEquals("SUV", document.getString("vehicleBodyStyle")),
                () -> assertEquals(28500.99, document.getDouble("vehiclePrice")),
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