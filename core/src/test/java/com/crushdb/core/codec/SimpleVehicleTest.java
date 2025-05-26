package com.crushdb.core.codec;

import com.crushdb.core.annotations.CrushId;

public class SimpleVehicleTest {

    @CrushId(include = true)
    private long id;
    private String vehicleMake;
    private String vehicleModel;
    private int vehicleYear;
    private String vehicleType;
    private String vehicleBodyStyle;
    private double vehiclePrice;
    private boolean hasHeating;

    public SimpleVehicleTest() {}

    public SimpleVehicleTest(
            long id,
            String vehicleMake,
            String vehicleModel,
            int vehicleYear,
            String vehicleType,
            String vehicleBodyStyle,
            double vehiclePrice,
            boolean hasHeating) {
        this.id = id;
        this.vehicleMake = vehicleMake;
        this.vehicleModel = vehicleModel;
        this.vehicleYear = vehicleYear;
        this.vehicleType = vehicleType;
        this.vehicleBodyStyle = vehicleBodyStyle;
        this.vehiclePrice = vehiclePrice;
        this.hasHeating = hasHeating;
    }

    public String getVehicleType() {
        return vehicleType;
    }

    public void setVehicleType(String vehicleType) {
        this.vehicleType = vehicleType;
    }

    public String getVehicleMake() {
        return vehicleMake;
    }

    public void setVehicleMake(String vehicleMake) {
        this.vehicleMake = vehicleMake;
    }

    public String getVehicleModel() {
        return vehicleModel;
    }

    public void setVehicleModel(String vehicleModel) {
        this.vehicleModel = vehicleModel;
    }

    public int getVehicleYear() {
        return vehicleYear;
    }

    public void setVehicleYear(int vehicleYear) {
        this.vehicleYear = vehicleYear;
    }

    public String getVehicleBodyStyle() {
        return vehicleBodyStyle;
    }

    public void setVehicleBodyStyle(String vehicleBodyStyle) {
        this.vehicleBodyStyle = vehicleBodyStyle;
    }

    public double getVehiclePrice() {
        return vehiclePrice;
    }

    public void setVehiclePrice(double vehiclePrice) {
        this.vehiclePrice = vehiclePrice;
    }

    public boolean hasHeating() {
        return hasHeating;
    }

    public void setHasHeating(boolean hasHeating) {
        this.hasHeating = hasHeating;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
