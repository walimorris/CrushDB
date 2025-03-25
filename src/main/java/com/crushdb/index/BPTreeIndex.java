package com.crushdb.index;

import com.crushdb.index.btree.BPTree;
import com.crushdb.index.btree.PageOffsetReference;
import com.crushdb.index.btree.SortOrder;

import java.util.List;
import java.util.Map;

/**
 * Represents a tree index with optional uniqueness constraint.
 * Each index is backed by a {@link BPTree} instance, and supports insert and search operations.
 *
 * @param <T> The type of key used for indexing (must be {@link Comparable}).
 */
public class BPTreeIndex<T extends Comparable<T>> {
    private final BPTree<T> tree;
    private final boolean unique;

    /**
     * Constructs a {@link BPTreeIndex} with the given configuration.
     *
     * @param unique does the index enforces uniqueness for keys
     * @param order the order of the tree
     * @param sortOrder the sort order
     */
    public BPTreeIndex(boolean unique, int order, SortOrder sortOrder) {
        this.tree = new BPTree<>(order, sortOrder);
        this.unique = unique;
    }

    /**
     * Inserts a key-reference pair into the tree index.
     *
     * @param key the key to insert
     * @param ref the page-offset reference to associate with the key
     *
     * @return true if the insert was successful, false if not
     *
     * @throws DuplicateKeyException if the key already exists in a unique index
     */
    public boolean insert(T key, PageOffsetReference ref) throws DuplicateKeyException {
        return tree.insert(key, ref, unique);
    }

    /**
     * Searches for the given key in the index and returns its associated references.
     *
     * @param key the key to search for
     *
     * @return a list {@link PageOffsetReference} of page-offset references associated with the key
     */
    public List<PageOffsetReference> search(T key) {
        return tree.search(key);
    }

    public Map<T, List<PageOffsetReference>> rangeSearch(T lowerBound, T upperBound) {
        return tree.rangeSearch(lowerBound, upperBound);
    }

    /**
     * Checks whether the index enforces uniqueness.
     *
     * @return true if the index is unique, false otherwise.
     */
    public boolean isUnique() {
        return this.unique;
    }

    /**
     * Returns the underlying tree object.
     *
     * @return the {@link BPTree} instance backing this index
     */
    public BPTree<T> getTree() {
        return this.tree;
    }
}
