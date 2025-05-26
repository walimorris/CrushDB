package com.crushdb.core.codec;

import com.crushdb.core.model.document.Document;

import java.util.Objects;

public class SimpleVehicleTestCodec implements Codec<SimpleVehicleTest> {

    public static final String VEHICLE_MAKE = "vehicleMake";
    public static final String VEHICLE_MODEL = "vehicleModel";
    public static final String VEHICLE_TYPE = "vehicleType";
    public static final String VEHICLE_PRICE = "vehiclePrice";
    public static final String VEHICLE_YEAR = "vehicleYear";
    public static final String VEHICLE_BODY_STYLE = "vehicleBodyStyle";
    public static final String HAS_HEATING = "hasHeating";

    @Override
    public Document encode(SimpleVehicleTest o) {
        if (Objects.isNull(o)) {
            throw new IllegalArgumentException("Object to encode cannot be null.");
        }
        Document document = new Document();
        document.put(VEHICLE_MAKE, o.getVehicleMake());
        document.put(VEHICLE_MODEL, o.getVehicleModel());
        document.put(VEHICLE_YEAR, o.getVehicleYear());
        document.put(VEHICLE_TYPE, o.getVehicleType());
        document.put(VEHICLE_BODY_STYLE, o.getVehicleBodyStyle());
        document.put(VEHICLE_PRICE, o.getVehiclePrice());
        document.put(HAS_HEATING, o.hasHeating());
        return document;
    }

    @Override
    public SimpleVehicleTest decode(Document document) {
        long id = document.getDocumentId();
        String vehicleMake = document.getString(VEHICLE_MAKE);
        String vehicleModel = document.getString(VEHICLE_MODEL);
        int vehicleYear = document.getInt(VEHICLE_YEAR);
        String vehicleType = document.getString(VEHICLE_TYPE);
        String vehicleBodyStyle = document.getString(VEHICLE_BODY_STYLE);
        double vehiclePrice = document.getDouble(VEHICLE_PRICE);
        boolean hasHeating = document.getBoolean(HAS_HEATING);

        return new SimpleVehicleTest(
                id,
                vehicleMake,
                vehicleModel,
                vehicleYear,
                vehicleType,
                vehicleBodyStyle,
                vehiclePrice,
                hasHeating
        );
    }

    @Override
    public Class<SimpleVehicleTest> getEncoderClass() {
        return SimpleVehicleTest.class;
    }
}
