package com.crushdb.core.model.document;

/**
 * {@code BsonType} represents BSON data types in CrushDB. Each type contains its respective
 * byte code. The code simply represents the type's binary representation. In this case when
 * a BsonTYpe is serialized or deserialized, a tool such as {@link java.nio.ByteBuffer}
 * knows what should be read.
 * <p>
 *     Take an example to read a Long data type:
 *     {@code ByteBuffer buffer = ByteBuffer.allocate(1024);
 *     long var = buffer.getLong();}
 *     In this case, a {@link java.nio.ByteBuffer} can be
 *     used to extract the Document's field with such a type.
 * </p>
 * In short, BSON is a binary representation of JSON-like documents. Each BsonType maps to a
 * specific BSON type identifier. This is generally more performant to the plain alternative.
 * Provides speed by design with efficient parsing and decoding, supports mixed types (arrays, objects).
 * each field knows its types so there's no schema needed at decode time.
 * <br><br>
 * {@code Supported Types:}
 * <ul>
 *     <li>{@link BsonType#STRING}</li>
 *     <li>{@link BsonType#INT}</li>
 *     <li>{@link BsonType#LONG}</li>
 *     <li>{@link BsonType#FLOAT}</li>
 *     <li>{@link BsonType#DOUBLE}</li>
 *     <li>{@link BsonType#BOOLEAN}</li>
 * </ul>
 */
public enum BsonType {
    STRING((byte) 0x01, String.class),
    INT((byte) 0x02, Integer.class),
    LONG((byte) 0x03, Long.class),
    BOOLEAN((byte) 0x08, Boolean.class),
    FLOAT((byte) 0x09, Float.class),
    DOUBLE((byte) 0x0A, Double.class);

    /**
     * Represents the unique byte code associated with a specific BSON type. Each BSON type is
     * mapped to a byte value that serves as its type identifier in the binary representation of
     * BSON documents.
     * <br>
     * This byte value is utilized during serialization and deserialization processes to identify
     * the exact BSON type and interpret the corresponding binary data correctly.
     */
    private final byte code;

    private final Class<? extends Comparable<?>> javaType;

    /**
     * Constructs a {@code BsonType} with the specified byte code.
     *
     * @param code the unique byte code representing the BSON type. This byte code is used
     *             to identify the type during serialization and deserialization processes
     */
    BsonType(byte code, Class<? extends Comparable<?>> javaType) {
        this.code = code;
        this.javaType = javaType;
    }

    /**
     * Get the unique byte code associated with the BSON type.
     *
     * @return the byte code representing the BSON type
     */
    public byte getCode() {
        return this.code;
    }

    public Class<? extends Comparable<?>> getJavaType() {
        return javaType;
    }

    /**
     * Converts a byte code to its corresponding {@code BsonType}.
     *
     * Iterates through all {@code BsonType} values to find the one associated with
     * the provided byte code. If no match is found, it throws an exception.
     *
     * @param code the byte code to be converted to a {@code BsonType}
     *
     * @return the corresponding {@code BsonType} for the specified byte code
     *
     * @throws IllegalArgumentException if the byte code does not match any {@code BsonType}
     */
    public static BsonType fromByte(byte code) {
        for (BsonType type : BsonType.values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown BsonType code: " + code);
    }
}
