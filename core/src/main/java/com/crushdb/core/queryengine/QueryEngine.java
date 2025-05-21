package com.crushdb.core.queryengine;

import com.crushdb.core.model.crate.CrateManager;
import com.crushdb.core.model.document.Document;
import com.crushdb.core.queryengine.executor.QueryExecutor;
import com.crushdb.core.queryengine.parser.QueryExpression;
import com.crushdb.core.queryengine.parser.QueryParser;
import com.crushdb.core.queryengine.planner.QueryPlan;
import com.crushdb.core.queryengine.planner.QueryPlanner;

import java.util.List;
import java.util.Map;

/**
 * The {@code QueryEngine} class is responsible for orchestrating the query processing pipeline.
 * It integrates query parsing, planning, and execution to provide a high-level API for querying
 * documents from data crates. The query pipeline consists of the following steps:
 * <p>
 *     <ol>
 *         <li>Query Parsing: Converts raw query input into a structured list of query expressions using {@link QueryParser}</li>
 *         <li>Query Planning: Generates optimized query plans from the parsed query expressions using {@link QueryPlanner}</li>
 *         <li>Query Execution: Executes the query plans and retrieves the resulting documents using {@link QueryExecutor}</li>
 *     </ol>
 * <p>
 */
public class QueryEngine {

    /**
     * Represents the query parsing component of the {@code QueryEngine}. Responsible for converting
     * raw query input into a structured list of {@link QueryExpression} objects using the
     * {@link QueryParser}.
     */
    private final QueryParser queryParser;

    /**
     * Represents the query planning component of the {@code QueryEngine}. Responsible for generating
     * optimized query execution plans based on parsed query expressions and the target crate. Utilizes
     * the {@link QueryPlanner} to analyze the query, determine applicable indexes, and create plans
     * that can be executed by the {@link QueryExecutor}.
     *
     */
    private final QueryPlanner queryPlanner;

    /**
     * Represents the query execution component of the {@code QueryEngine}. Responsible for executing
     * prepared query plans and retrieving the resulting documents. Utilizes the {@link QueryExecutor}
     * to process query plans, evaluate expressions, and perform result aggregation.
     */
    private final QueryExecutor queryExecutor;

    public QueryEngine(QueryParser queryParser, QueryPlanner queryPlanner, QueryExecutor queryExecutor) {
        this.queryParser = queryParser;
        this.queryPlanner = queryPlanner;
        this.queryExecutor = queryExecutor;
    }

    /**
     * Executes a query on the specified crate by processing the raw query input
     * through the query parsing, planning, and execution pipeline.
     *
     * @param crateName the name of the crate to query; identifies the target data crate
     * @param rawQuery a map representing the raw query input; contains key-value pairs
     *        defining query criteria
     *
     * @return a list of {@link Document} objects that match the query criteria
     */
    public List<Document> find(String crateName, Map<String, Object> rawQuery) {
        List<QueryExpression> expressions = queryParser.parse(rawQuery);
        List<QueryPlan> plans = queryPlanner.plan(crateName, expressions);
        return queryExecutor.execute(plans);
    }

    /**
     * Get {@code CrateManager}.
     *
     * @return {@link CrateManager}
     */
    public CrateManager getCrateManager() {
        return this.queryPlanner.getCrateManager();
    }
}
