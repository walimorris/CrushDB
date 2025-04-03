package com.crushdb.index;

import com.crushdb.index.btree.BPTree;
import com.crushdb.index.btree.PageOffsetReference;
import com.crushdb.index.btree.SortOrder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages multiple named {@link BPTree } indexes, providing an interface for creation, insertion, and search.
 * TODO: Add crate name for logical grouping
 *
 * @param <T> The type of key used for indexing (must be Comparable).
 */
public class BPTreeIndexManager<T extends Comparable<T>> {

    /**
     * A map that stores named tree indexes managed by the BPTreeIndexManager.
     * The keys in the map represent index names, and the values are corresponding
     * instances of {@link BPTreeIndex}.
     */
    private final Map<String, BPTreeIndex<T>> indexes = new HashMap<>();

    private static final String INDEX_NOT_FOUND = "Index not found: ";

    /**
     * Creates a new index and stores it by name.
     *
     * @param indexName the name of the index
     * @param fieldName the name of the indexed field
     * @param unique should enforce uniqueness for keys
     * @param order the order of the tree
     * @param sortOrder sort order
     */
    public BPTreeIndex<T> createIndex(String indexName, String fieldName, boolean unique, int order, SortOrder sortOrder) {
        indexes.put(indexName, new BPTreeIndex<>(indexName, fieldName, unique, order, sortOrder));
        return getIndex(indexName);
    }

    /**
     * Inserts a key-reference pair into the specified index.
     *
     * @param indexName the name of the index to insert into
     * @param key the key to insert
     * @param reference the page-offset reference to associate with the key
     *
     * @return true if the insert was successful, false if not
     *
     * @throws DuplicateKeyException if the key already exists in a unique index
     * @throws IllegalArgumentException if the index does not exist
     */
    public boolean insert(String indexName, T key, PageOffsetReference reference) throws DuplicateKeyException {
        BPTreeIndex<T> index = indexes.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException(INDEX_NOT_FOUND + indexName);
        }
        return index.insert(key, reference);
    }

    /**
     * Inserts a key-reference pair into the specified index.
     *
     * @param indexName the name of the index to insert into
     * @param indexEntry the {@link IndexEntry}
     *
     * @return boolean true if the insert was successful, false if not
     *
     * @throws DuplicateKeyException if the key already exists in a unique index
     * @throws IllegalArgumentException if the index does not exist
     */
    public boolean insert(String indexName, IndexEntry<T> indexEntry) throws DuplicateKeyException {
        BPTreeIndex<T> index = indexes.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException(INDEX_NOT_FOUND + indexName);
        }
        return index.insert(indexEntry.key(), indexEntry.reference());
    }

    /**
     * Searches the specified index for a key.
     *
     * @param indexName the name of the index to search
     * @param key the key to search for
     *
     * @return a list of page-offset references associated with the key
     *
     * @throws IllegalArgumentException if the index does not exist
     */
    public List<PageOffsetReference> search(String indexName, T key) {
        BPTreeIndex<T> index = indexes.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException(INDEX_NOT_FOUND + indexName);
        }
        return index.search(key);
    }

    /**
     * Searches the specified index for a key.
     *
     * @param indexName the name of the index to search
     * @param indexEntry the {@link IndexEntry}
     *
     * @return a list of page-offset references associated with the key
     *
     * @throws IllegalArgumentException if the index does not exist
     */
    public List<PageOffsetReference> search(String indexName, IndexEntry<T> indexEntry) {
        BPTreeIndex<T> index = indexes.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException(INDEX_NOT_FOUND + indexName);
        }
        return index.search(indexEntry.key());
    }

    /**
     * Performs a range search on the tree, retrieving all keys and their associated references
     * that fall within the specified range [lowerBound, upperBound], inclusive.
     *
     * @param indexName the name of the index being searched
     * @param lowerBound the minimum key value (inclusive) for the range search
     * @param upperBound the maximum key value (inclusive) for the range search
     *
     * @return a map where each key within the range is associated with a list of {@link PageOffsetReference},
     *         or {@code null} if no keys fall within the specified range
     */
    public Map<T, List<PageOffsetReference>> rangeSearch(String indexName, T lowerBound, T upperBound) {
        BPTreeIndex<T> index = indexes.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException(INDEX_NOT_FOUND + indexName);
        }
        return index.rangeSearch(lowerBound, upperBound);
    }

    /**
     * Get the map of all indexes managed by the BPTreeIndexManager.
     *
     * @return a map where the keys are index names (as Strings) and the values are
     *         {@link BPTreeIndex} instances associated with those names.
     */
    public Map<String, BPTreeIndex<T>> getIndexes() {
        return indexes;
    }

    /**
     * Returns the {@link BPTreeIndex} instance associated with the given name.
     *
     * @param name the name of the index
     *
     * @return the corresponding {@link BPTreeIndex}, or null if not found.
     */
    public BPTreeIndex<T> getIndex(String name) {
        return indexes.get(name);
    }
}
