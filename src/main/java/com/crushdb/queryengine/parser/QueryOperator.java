package com.crushdb.queryengine.parser;

/**
 * The {@code QueryOperator} enum defines a set of supported query operators
 * used to construct query expressions in CrushDB.
 * Each operator is associated with a specific symbol that is used in query definitions.
 * <p>
 * Supported operators include:
 * - EQUALS ("$eq"): Represents an equality condition.
 * - GREATER_THAN ("$gt"): Represents a greater-than condition.
 * - GREATER_THAN_OR_EQUAL_TO ("$gte"): Represents a greater-than-or-equal-to condition.
 * - LESS_THAN ("$lt"): Represents a less-than condition.
 * - LESS_THAN_OR_EQUAL_TO ("lte"): Represents a less-than-or-equal-to condition.
 * - NOT_EQUALS ("$not"): Represents a negation or inequality condition.
 * - AND ("$and"): Represents a logical AND condition.
 * - OR ("$or"): Represents a logical OR condition.
 * <p>
 * This enum also provides utility methods to retrieve an operator from its associated symbol.
 */
public enum QueryOperator {
    EQUALS("$eq"),
    GREATER_THAN("$gt"),
    GREATER_THAN_OR_EQUAL_TO("$gte"),
    LESS_THAN("$lt"),
    LESS_THAN_OR_EQUAL_TO("lte"),
    NOT_EQUALS("$not"),
    AND("$and"),
    OR("$or");

    private final String symbol;

    QueryOperator(String symbol) {
        this.symbol = symbol;
    }

    /**
     * Retrieves a {@link QueryOperator} associated with the provided symbol.
     * If the symbol does not match any of the defined operators, an
     * {@link IllegalArgumentException} is thrown.
     *
     * @param symbol the string representation of the operator symbol
     * @return the {@link QueryOperator} corresponding to the given symbol
     * @throws IllegalArgumentException if the symbol does not match any defined operator
     */
    public static QueryOperator fromSymbol(String symbol) {
        for (QueryOperator op : values()) {
            if (op.symbol.equals(symbol)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unsupported operator: " + symbol);
    }
}
