package com.crushdb.queryengine.executor;

import com.crushdb.logger.CrushDBLogger;
import com.crushdb.model.document.Document;
import com.crushdb.queryengine.planner.QueryPlan;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The QueryExecutor class is responsible for executing a list of query plans and returning
 * the resulting documents. It processes the query plans, evaluates the queries individually,
 * and performs an intersection of the results for an AND logic operation.
 */
public class QueryExecutor {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(QueryExecutor.class);

    /**
     * Executes a list of query plans, evaluates each query separately, and returns the
     * intersection of results adhering to an AND logic operation.
     *
     * @param plans a list of {@link QueryPlan} objects where each plan represents a query
     *              to be executed
     *
     * @return a list of {@link Document} objects resulting from the intersection of
     *         individual query results
     */
    public List<Document> execute(List<QueryPlan> plans) {
        if (plans.isEmpty()) {
            return List.of();
        }

        // step 1: run each plan individually - useIndex will be used in explainPlan
        List<HashSet<Document>> resultSets = plans.stream()
                .map(plan -> new HashSet<>(plan.getCrate().find(
                        plan.getQueryExpression().getField(),
                        plan.getQueryExpression().getValue()
                )))
                .toList();

        // step 2: intersect the sets for AND logic
        Set<Document> intersection = resultSets.get(0);
        for (int i = 1; i < resultSets.size(); i++) {
            intersection.retainAll(resultSets.get(i));
        }
        return List.copyOf(intersection);
    }
}
