package com.crushdb.queryengine.parser;

import com.crushdb.logger.CrushDBLogger;
import com.crushdb.model.document.BsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The {@code QueryParser} is responsible for parsing query maps into a list of query expressions.
 * Each query expression represents a field, a value, and an operator that can be used to
 * construct queries for processing or database lookups.
 * <p>
 * The supported operators and their corresponding symbols are defined in the {@link QueryOperator} enum.
 *<p>
 * This class is designed to handle both simple equality checks and complex queries involving
 * operators such as greater-than, less-than, etc.
 */
public class QueryParser {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(QueryParser.class);

    /**
     * Parses a map-based query into a list of {@link QueryExpression} objects.
     * Each entry in the input map represents a field and its corresponding value,
     * which can either be a direct value for equality checks or a nested map
     * specifying an operator and value pair.
     * <p>
     *     Example:
     *     {@code Map<String, Object> query1 = Map.of("vehicle_make", Map.of("$eq", "Subaru"));}
     * </p>
     *
     * @param query a map where the key represents the field name and the value
     *              is either a direct value for equality or a nested map representing
     *              operator-value pairs for more complex queries.
     * @return a list of {@link QueryExpression} objects, where each expression
     *         encapsulates a field, value, and operator derived from the input map.
     */
    public List<QueryExpression> parse(Map<String, Object> query) {
        List<QueryExpression> expressions = new ArrayList<>();

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof Map<?, ?> operatorMap) {
                for (Map.Entry<?, ?> opEntry : operatorMap.entrySet()) {
                    String opSymbol = opEntry.getKey().toString();
                    Object rawValue = opEntry.getValue();

                    QueryOperator operator = QueryOperator.fromSymbol(opSymbol);
                    BsonValue bsonValue = BsonValue.fromObject(rawValue);

                    expressions.add(new QueryExpression(field, bsonValue, operator));
                }
            } else {

                // equality default
                // TODO: clean up
                BsonValue bsonValue = BsonValue.fromObject(value);
                expressions.add(new QueryExpression(field, bsonValue, QueryOperator.EQUALS));
            }
        }
        return expressions;
    }
}
