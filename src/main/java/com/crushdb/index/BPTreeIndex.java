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

    /**
     * Represents the underlying B+ Tree structure used by the {@code BPTreeIndex} class
     * for indexing and managing key-reference pairs.
     *
     * {@code <T> the type of keys stored in the tree, which must be comparable}
     */
    private final BPTree<T> tree;

    /**
     * The name of the tree-based index.
     */
    private final String indexName;

    /**
     * Represents the name of the field being indexed.
     */
    private final String fieldName;

    /**
     * Indicates whether the tree index enforces uniqueness for its keys.
     */
    private final boolean unique;

    /**
     * Constructs a {@link BPTreeIndex} with the given configuration.
     *
     * @param indexName name of the tree index
     * @param fieldName name of the field being indexed
     * @param unique does the index enforces uniqueness for keys
     * @param order the order of the tree
     * @param sortOrder the sort order
     */
    public BPTreeIndex(String indexName, String fieldName, boolean unique, int order, SortOrder sortOrder) {
        this.indexName = indexName;
        this.fieldName = fieldName;
        this.tree = new BPTree<>(order, sortOrder);
        this.unique = unique;
    }

    /**
     * Inserts a key-reference pair into the tree index.
     *
     * @param key the key to insert
     * @param ref the page-offset reference to associate with the key
     *
     * @return boolean true if the insert was successful, false if not
     *
     * @throws DuplicateKeyException if the key already exists in a unique index
     */
    public boolean insert(T key, PageOffsetReference ref) throws DuplicateKeyException {
        return tree.insert(key, ref, this.unique);
    }

    /**
     * Inserts a key-reference pair into the tree index.
     *
     * @param indexEntry {@link IndexEntry} contains the key and reference
     *
     * @return boolean true if the insert was successful, false if not
     *
     * @throws DuplicateKeyException if the key already exists in a unique index
     */
    public boolean insert(IndexEntry<T> indexEntry) throws DuplicateKeyException {
        return tree.insert(indexEntry.key(), indexEntry.reference(), this.unique);
    }

    /**
     * Searches for the given key in the index and returns its associated references.
     *
     * @param key the key to search for
     *
     * @return a list of {@link PageOffsetReference} of page-offset references associated with the key
     */
    public List<PageOffsetReference> search(T key) {
        return tree.search(key);
    }

    /**
     * Searches for the key in the index and returns its associated references
     * given a {@link IndexEntry}.
     *
     * @param indexEntry the {@link IndexEntry} containing the key to search for
     *
     * @return a list of {@link PageOffsetReference} associated with the key
     */
    public List<PageOffsetReference> search(IndexEntry<T> indexEntry) {
        return tree.search(indexEntry.key());
    }

    /**
     * Performs a range search on the tree, retrieving all keys and their associated references
     * that fall within the specified range [lowerBound, upperBound], inclusive.
     *
     * @param lowerBound the minimum key value (inclusive) for the range search
     * @param upperBound the maximum key value (inclusive) for the range search
     *
     * @return a map where each key within the range is associated with a list of {@link PageOffsetReference},
     *         or {@code null} if no keys fall within the specified range
     */
    public Map<T, List<PageOffsetReference>> rangeSearch(T lowerBound, T upperBound) {
        return tree.rangeSearch(lowerBound, upperBound);
    }

    /**
     * Get the name of the index.
     *
     * @return the name of the index as a {@code String}.
     */
    public String getIndexName() {
        return this.indexName;
    }

    /**
     * Get the name of the indexed field.
     *
     * @return the name of the field associated with this index as a {@code String}.
     */
    public String getFieldName() {
        return this.fieldName;
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
