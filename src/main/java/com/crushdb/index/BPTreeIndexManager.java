package com.crushdb.index;

import com.crushdb.index.btree.BPTree;
import com.crushdb.index.btree.PageOffsetReference;
import com.crushdb.index.btree.SortOrder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages multiple named {@link BPTree } indexes, providing an interface for creation, insertion, and search.
 *
 * @param <T> The type of key used for indexing (must be Comparable).
 */
public class BPTreeIndexManager<T extends Comparable<T>> {
    private final Map<String, BPTreeIndex<T>> indexes = new HashMap<>();

    /**
     * Creates a new index and stores it by name.
     *
     * @param name the name of the index
     * @param unique should enforce uniqueness for keys
     * @param order the order of the tree
     * @param sortOrder sort order
     */
    public void createIndex(String name, boolean unique, int order, SortOrder sortOrder) {
        indexes.put(name, new BPTreeIndex<>(unique, order, sortOrder));
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
            throw new IllegalArgumentException("Index not found: " + indexName);
        }
        return index.insert(key, reference);
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
            throw new IllegalArgumentException("Index not found: " + indexName);
        }
        return index.search(key);
    }

    public Map<T, List<PageOffsetReference>> rangeSearch(String indexName, T lowerBound, T upperBound) {
        BPTreeIndex<T> index = indexes.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException("Index not found: " + indexName);
        }
        return index.rangeSearch(lowerBound, upperBound);
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
