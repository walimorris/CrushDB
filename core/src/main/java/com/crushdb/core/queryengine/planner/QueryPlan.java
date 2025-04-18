package com.crushdb.core.queryengine.planner;

import com.crushdb.core.index.BPTreeIndex;
import com.crushdb.core.model.crate.Crate;
import com.crushdb.core.queryengine.parser.QueryExpression;

import java.util.Optional;

/**
 * Represents a plan for executing a query in CrushDB. A {@code QueryPlan}
 * specifies the target {@link  Crate}, the {@link QueryExpression} used for filtering,
 * whether an index is utilized, and optionally, the specific {@link BPTreeIndex}
 * to be used for query optimization.
 */
public class QueryPlan {
    private final Crate crate;
    private final QueryExpression queryExpression;
    private final boolean useIndex;
    private final Optional<BPTreeIndex<?>> index;

    public QueryPlan(Crate crate, QueryExpression queryExpression, boolean useIndex, Optional<BPTreeIndex<?>> index) {
        this.crate = crate;
        this.queryExpression = queryExpression;
        this.useIndex = useIndex;
        this.index = index;
    }

    /**
     * Get the {@link QueryExpression} associated with this {@code QueryPlan}.
     *
     * @return the {@code QueryExpression} used to filter results within a query plan
     */
    public QueryExpression getQueryExpression() {
        return queryExpression;
    }

    /**
     * Get the {@link Crate} associated with this {@code QueryPlan}.
     *
     * @return the {@link Crate} that serves as the target for this query plan
     */
    public Crate getCrate() {
        return crate;
    }

    /**
     * Determines whether the query plan is configured to use an index for query optimization.
     *
     * @return {@code true} if the query plan is utilizing an index; {@code false} otherwise
     */
    public boolean isUseIndex() {
        return useIndex;
    }

    /**
     * Returns the {@link Optional} instance of {@link BPTreeIndex},
     * representing the index associated with this {@code QueryPlan}.
     *
     * @return an {@link Optional} containing the {@link BPTreeIndex} if an index is associated;
     *         otherwise, an empty {@link Optional}.
     */
    public Optional<BPTreeIndex<?>> getIndex() {
        return index;
    }
}
