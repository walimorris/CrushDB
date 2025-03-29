package com.crushdb.model;

public class BsonValue {
    private final BsonType bsonType;
    private final Object value;

    public BsonValue(BsonType bsonType, Object value) {
        this.bsonType = bsonType;
        this.value = value;
    }

    public static BsonValue ofString(String value) {
        return new BsonValue(BsonType.STRING, value);
    }

    public String asString() {
        return (String) this.value;
    }

    public static BsonValue ofInteger(int value) {
        return new BsonValue(BsonType.INT, value);
    }

    public int asInteger() {
        return (Integer) this.value;
    }

    public static BsonValue ofLong(long value) {
        return new BsonValue(BsonType.LONG, value);
    }

    public long asLong() {
        return (Long) this.value;
    }

    public static BsonValue ofBoolean(boolean value) {
        return new BsonValue(BsonType.BOOLEAN, value);
    }

    public boolean asBoolean() {
        return (Boolean) this.value;
    }

    public static BsonValue ofFloat(float value) {
        return new BsonValue(BsonType.FLOAT, value);
    }

    public float asFloat() {
        return (Float) this.value;
    }

    public static BsonValue ofDouble(double value) {
        return new BsonValue(BsonType.DOUBLE, value);
    }

    public double asDouble() {
        return (Double) this.value;
    }
}
