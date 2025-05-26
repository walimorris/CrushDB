package com.crushdb.core.codec;

import com.crushdb.core.TestUtil;
import com.crushdb.core.model.document.Document;
import org.junit.jupiter.api.Test;

import static com.crushdb.core.codec.SimpleVehicleTestCodec.*;
import static org.junit.jupiter.api.Assertions.*;

class CodecTest {
    private final SimpleVehicleTestCodec simpleVehicleTestCodec = new SimpleVehicleTestCodec();

    private static final String SimpleVehicleTestJson = TestUtil.loadResourceAsString("SimpleVehicleTest.json");

    @Test
    void encode() {
        SimpleVehicleTest simpleVehicleTestObj = new SimpleVehicleTest();
        simpleVehicleTestObj.setVehicleMake("Tesla");
        simpleVehicleTestObj.setVehicleModel("Model3");
        simpleVehicleTestObj.setVehicleType("automatic");
        simpleVehicleTestObj.setVehiclePrice(30000.99);
        simpleVehicleTestObj.setVehicleYear(2017);
        simpleVehicleTestObj.setVehicleBodyStyle("Sudan");
        simpleVehicleTestObj.setHasHeating(true);
        simpleVehicleTestObj.setId(100);

        Document simpleVehicleTestDocument = simpleVehicleTestCodec
                .encode(simpleVehicleTestObj);

        System.out.println(simpleVehicleTestDocument.getDocumentId());
        assertInstanceOf(Long.class, simpleVehicleTestDocument.getDocumentId());
        assertEquals(simpleVehicleTestObj.getVehicleMake(), simpleVehicleTestDocument.getString(VEHICLE_MAKE));
        assertEquals(simpleVehicleTestObj.getVehicleModel(), simpleVehicleTestDocument.getString(VEHICLE_MODEL));
        assertEquals(simpleVehicleTestObj.getVehicleType(), simpleVehicleTestDocument.getString(VEHICLE_TYPE));
        assertEquals(simpleVehicleTestObj.getVehiclePrice(), simpleVehicleTestDocument.getDouble(VEHICLE_PRICE));
        assertEquals(simpleVehicleTestObj.getVehicleYear(), simpleVehicleTestDocument.getInt(VEHICLE_YEAR));
        assertEquals(simpleVehicleTestObj.getVehicleBodyStyle(), simpleVehicleTestDocument.getString(VEHICLE_BODY_STYLE));
        assertEquals(simpleVehicleTestObj.hasHeating(), simpleVehicleTestDocument.getBoolean(HAS_HEATING));
    }

    @Test
    void decode() {
        Document simpleVehicleTestDocument = Document.fromSimpleJson(SimpleVehicleTestJson);

        assertNotNull(simpleVehicleTestDocument);
        assertInstanceOf(Long.class, simpleVehicleTestDocument.getDocumentId());

        SimpleVehicleTest simpleVehicleTestObj = simpleVehicleTestCodec.decode(simpleVehicleTestDocument);
        assertInstanceOf(Long.class, simpleVehicleTestObj.getId());
        assertEquals(simpleVehicleTestDocument.getString(VEHICLE_MAKE), simpleVehicleTestObj.getVehicleMake());
        assertEquals(simpleVehicleTestDocument.getString(VEHICLE_MODEL), simpleVehicleTestObj.getVehicleModel());
        assertEquals(simpleVehicleTestDocument.getString(VEHICLE_TYPE), simpleVehicleTestObj.getVehicleType());
        assertEquals(simpleVehicleTestDocument.getDouble(VEHICLE_PRICE), simpleVehicleTestObj.getVehiclePrice());
        assertEquals(simpleVehicleTestDocument.getInt(VEHICLE_YEAR), simpleVehicleTestObj.getVehicleYear());
        assertEquals(simpleVehicleTestDocument.getString(VEHICLE_BODY_STYLE), simpleVehicleTestObj.getVehicleBodyStyle());
        assertEquals(simpleVehicleTestDocument.getBoolean(HAS_HEATING), simpleVehicleTestObj.hasHeating());
    }

    @Test
    void getEncoderClass() {
        assertEquals(SimpleVehicleTest.class, simpleVehicleTestCodec.getEncoderClass());
    }
}