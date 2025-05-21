package com.crushdb.queryengine.planner;

import com.crushdb.index.BPTreeIndex;
import com.crushdb.logger.CrushDBLogger;
import com.crushdb.model.crate.Crate;
import com.crushdb.model.crate.CrateManager;
import com.crushdb.queryengine.parser.QueryExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The {@code QueryPlanner} is responsible for generating query plans for a given set
 * of query expressions. It interacts with the CrateManager to retrieve the relevant crate
 * and determines if available indexes can be used to optimize query execution.
 */
public class QueryPlanner {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(QueryPlanner.class);

    /**
     * Manages interaction with crates in the CrushDB ecosystem, including retrieval and
     * registration of crates. Serves as a critical dependency for the query planning process
     * by providing access to crate-related metadata and operations.
     * <p>
     * Used in the {@code QueryPlanner} class to fetch crates and their indexes, enabling
     * query optimization and execution planning.
     * <p>
     */
    private final CrateManager crateManager;

    public QueryPlanner(CrateManager crateManager) {
        this.crateManager = crateManager;
    }

    /**
     * Generates a list of query plans based on the target crate and a list of query expressions.
     * Each query plan specifies the target crate, the query expression used for filtering,
     * whether an index will be utilized, and the associated index if available.
     *
     * @param crateName the name of the crate to query
     * @param expressions the list of query expressions to be used for constructing query plans
     * @return a list of {@code QueryPlan} objects, each representing an execution plan
     *         for the given query expressions on the specified crate
     */
    public List<QueryPlan> plan(String crateName, List<QueryExpression> expressions) {
        Crate crate = crateManager.getCrate(crateName);
        List<QueryPlan> plans = new ArrayList<>();

        for (QueryExpression expr : expressions) {
            Optional<BPTreeIndex<?>> index = crate.getIndex(expr.getField());
            boolean useIndex = index.isPresent();
            plans.add(new QueryPlan(crate, expr, useIndex, index));
        }
        return plans;
    }

    /**
     * Get {@code CrateManager}.
     *
     * @return {@link CrateManager}
     */
    public CrateManager getCrateManager() {
        return this.crateManager;
    }
}
