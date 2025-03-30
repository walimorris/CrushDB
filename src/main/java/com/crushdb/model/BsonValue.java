package com.crushdb.model;

/**
 * Represents a BSON value that consists of a {@link BsonType} indicating the type of the value
 * and the corresponding actual value.
 * <br>
 * This class provides static factory methods for creating instances of different BSON types
 * (e.g., {@link BsonType#STRING}, {@link BsonType#INT}, etc.) with corresponding values. It also
 * provides methods to retrieve the values in their specific types.
 *<br>
 * Instances of this class are immutable and represents typed values.
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
public record BsonValue(BsonType bsonType, Object value) {

    /**
     * Creates a {@link BsonValue} instance representing a BSON string value.
     *
     * @param value the string value to be encapsulated as a BSON value
     *
     * @return a {@link BsonValue} instance with a BSON type of {@link BsonType#STRING}
     */
    public static BsonValue ofString(String value) {
        return new BsonValue(BsonType.STRING, value);
    }

    /**
     * Retrieves the underlying value of this {@code BsonValue} as a {@code String}.
     *
     * @return the {@code String} representation of the underlying value
     *         if this {@code BsonValue} instance represents a BSON string type
     *
     * @throws ClassCastException if the underlying value is not of type {@code String}
     */
    public String asString() throws ClassCastException {
        return (String) this.value;
    }

    /**
     * Creates a {@link BsonValue} instance representing a BSON integer value.
     *
     * @param value the integer value to be encapsulated as a BSON value
     *
     * @return a {@link BsonValue} instance with a BSON type of {@link BsonType#INT}
     */
    public static BsonValue ofInteger(int value) {
        return new BsonValue(BsonType.INT, value);
    }

    /**
     * Retrieves the underlying value of this {@code BsonValue} as an {@code int}.
     *
     * @return the {@code int} representation of the underlying value
     *         if this {@code BsonValue} instance represents a BSON integer type
     *
     * @throws ClassCastException if the underlying value is not of type {@code Integer}
     */
    public int asInteger() throws ClassCastException {
        return (Integer) this.value;
    }

    /**
     * Creates a {@link BsonValue} instance representing a BSON long value.
     *
     * @param value the long value to be encapsulated as a BSON value
     *
     * @return a {@link BsonValue} instance with a BSON type of {@link BsonType#LONG}
     */
    public static BsonValue ofLong(long value) {
        return new BsonValue(BsonType.LONG, value);
    }

    /**
     * Retrieves the underlying value of this {@code BsonValue} as a {@code long}.
     *
     * @return the {@code long} representation of the underlying value
     *         if this {@code BsonValue} instance represents a BSON long type
     *
     * @throws ClassCastException if the underlying value is not of type {@code Long}
     */
    public long asLong() throws ClassCastException {
        return (Long) this.value;
    }

    /**
     * Creates a {@link BsonValue} instance representing a BSON boolean value.
     *
     * @param value the boolean value to be encapsulated as a BSON value
     *
     * @return a {@link BsonValue} instance with a BSON type of {@link BsonType#BOOLEAN}
     */
    public static BsonValue ofBoolean(boolean value) {
        return new BsonValue(BsonType.BOOLEAN, value);
    }

    /**
     * Retrieves the underlying value of this {@code BsonValue} as a {@code boolean}.
     *
     * @return the {@code boolean} representation of the underlying value
     *         if this {@code BsonValue} instance represents a BSON boolean type
     *
     * @throws ClassCastException if the underlying value is not of type {@code Boolean}
     */
    public boolean asBoolean() throws ClassCastException {
        return (Boolean) this.value;
    }

    /**
     * Creates a {@link BsonValue} instance representing a BSON float value.
     *
     * @param value the float value to be encapsulated as a BSON value
     *
     * @return a {@link BsonValue} instance with a BSON type of {@link BsonType#FLOAT}
     */
    public static BsonValue ofFloat(float value) {
        return new BsonValue(BsonType.FLOAT, value);
    }

    /**
     * Retrieves the underlying value of this {@code BsonValue} as a {@code float}.
     *
     * @return the {@code float} representation of the underlying value
     *         if this {@code BsonValue} instance represents a BSON float type
     *
     * @throws ClassCastException if the underlying value is not of type {@code Float}
     */
    public float asFloat() throws ClassCastException {
        return (Float) this.value;
    }

    /**
     * Creates a {@link BsonValue} instance representing a BSON double value.
     *
     * @param value the double value to be encapsulated as a BSON value
     *
     * @return a {@link BsonValue} instance with a BSON type of {@link BsonType#DOUBLE}
     */
    public static BsonValue ofDouble(double value) {
        return new BsonValue(BsonType.DOUBLE, value);
    }

    /**
     * Retrieves the underlying value of this {@code BsonValue} as a {@code double}.
     *
     * @return the {@code double} representation of the underlying value
     *         if this {@code BsonValue} instance represents a BSON double type
     *
     * @throws ClassCastException if the underlying value is not of type {@code Double}
     */
    public double asDouble() throws ClassCastException {
        return (Double) this.value;
    }

    /**
     * Get the Java Data Type Value from the Bson Type.
     *
     * @return {@link Comparable}
     *
     * @throws UnsupportedOperationException if the type is not yet supported
     */
    public Comparable<?> toJavaValue() throws UnsupportedOperationException {
        return switch (this.bsonType()) {
            case STRING -> asString();
            case INT -> asInteger();
            case LONG -> asLong();
            case FLOAT -> asFloat();
            case DOUBLE -> asDouble();
            default -> throw new UnsupportedOperationException("Not Supported.");
        };
    }
}
