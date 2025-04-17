package com.crushdb.queryengine.parser;

import com.crushdb.model.document.BsonValue;

/**
 * Represents a query expression used to filter or match data based on specified criteria.
 * A query expression consists of a target field, a value to be compared, and an operator
 * that defines the comparison logic.
 * <p>
 * This class is immutable and provides accessors to retrieve the individual components
 * of the query expression.
 */
public class QueryExpression {

    private final String field;
    private final BsonValue value;
    private final QueryOperator operator;

    public QueryExpression(String field, BsonValue value, QueryOperator operator) {
        this.field = field;
        this.value = value;
        this.operator = operator;
    }

    /**
     * Get the BSON value associated with this query expression.
     *
     * @return the {@link BsonValue} representing the value to be compared in the query expression
     */
    public BsonValue getValue() {
        return value;
    }

    /**
     * Get the name of the field targeted by this query expression.
     *
     * @return the name of the field associated with this query expression
     */
    public String getField() {
        return field;
    }

    /**
     * Get the query operator associated with this query expression.
     *
     * @return the {@link QueryOperator} representing the operation to be applied
     *         in the query expression.
     */
    public QueryOperator getOperator() {
        return operator;
    }
}
