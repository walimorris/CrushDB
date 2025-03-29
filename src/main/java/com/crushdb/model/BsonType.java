package com.crushdb.model;

public enum BsonType {
    STRING((byte) 0x01),
    INT((byte) 0x02),
    LONG((byte) 0x03),
    BOOLEAN((byte) 0x08),
    FLOAT((byte) 0x09),
    DOUBLE((byte) 0x0A);

    private final byte code;

    BsonType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return this.code;
    }
}
