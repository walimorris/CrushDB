package com.crushdb.index;

import com.crushdb.index.btree.BPTree;
import com.crushdb.index.btree.PageOffsetReference;
import com.crushdb.index.btree.SortOrder;
import com.crushdb.logger.CrushDBLogger;
import com.crushdb.model.crate.Crate;
import com.crushdb.model.document.BsonType;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Manages multiple named {@link BPTree } indexes, providing an interface for creation, insertion, and search.
 */
public class BPTreeIndexManager {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(BPTreeIndexManager.class);

    private static BPTreeIndexManager instance;

    /**
     * A map that stores all indexes managed by the BPTreeIndexManager, organized by Crate names
     * and their corresponding index names.
     *
     * The outer map's keys represent Crate names as Strings, and the values are inner maps.
     * Each inner map's keys represent individual index names within a Crate, and the values
     * are instances of {@link BPTreeIndex} associated with those index names.
     *
     * This structure allows efficient organization and retrieval of indexes within different
     * logical groupings (Crates), where each crate can hold multiple indexes.
     *
     * @see Crate
     */
    private Map<String, Map<String, BPTreeIndex<?>>> crateIndexes;

    private static final String INDEX_NOT_FOUND = "Index not found: ";

    private BPTreeIndexManager() {
        init();
    }

    public static synchronized BPTreeIndexManager getInstance() {
        if (instance == null) {
            instance = new BPTreeIndexManager();
        }
        return instance;
    }

    private void init() {
        this.crateIndexes = new HashMap<>();
    }

    /**
     * Creates a new index and stores it by name inside the respective Crate.
     *
     * @param bsonType the index bson type
     * @param crateName the name of the crate
     * @param indexName the name of the index
     * @param fieldName the name of the indexed field
     * @param unique should enforce uniqueness for keys
     * @param order the order of the tree
     * @param sortOrder sort order
     */
    public <T extends Comparable<T>> BPTreeIndex<T> createIndex(BsonType bsonType, String crateName, String indexName, String fieldName, boolean unique, int order, SortOrder sortOrder) {
        BPTreeIndex<T> index = new BPTreeIndex<>(bsonType, crateName, indexName, fieldName, unique, order, sortOrder);
        crateIndexes.computeIfAbsent(crateName, key -> new HashMap<>())
                        .put(indexName, index);
        return index;
    }

    /**
     * Inserts a key-reference pair into the specified index.
     *
     * @param crateName the name of the crate
     * @param indexName the name of the index to insert into
     * @param key the key to insert
     * @param reference the page-offset reference to associate with the key
     *
     * @return true if the insert was successful, false if not
     *
     * @throws DuplicateKeyException if the key already exists in a unique index
     * @throws IllegalArgumentException if the index does not exist
     */
    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> boolean insert(String crateName, String indexName, T key, PageOffsetReference reference) throws DuplicateKeyException {
        BPTreeIndex<?> rawIndex = getIndex(crateName, indexName);
        if (rawIndex == null) {
            LOGGER.error(String.format("'%s' index not found for inserting key: '%s'", indexName, key), IllegalArgumentException.class.getName());
            throw new IllegalArgumentException(INDEX_NOT_FOUND + indexName);
        }

        if (!rawIndex.bsonType().equals(key)) {
            LOGGER.error(String.format("Mismatched key type. Expected: " +
                    rawIndex.bsonType() + ", Provided: " + key.getClass().getSimpleName()),
                    IllegalArgumentException.class.getName());

            throw new IllegalArgumentException("Mismatched key type. Expected: " +
                    rawIndex.bsonType() + ", Provided: " + key.getClass().getSimpleName());
        }
        BPTreeIndex<T> index = (BPTreeIndex<T>) rawIndex;
        return index.insert(key, reference);
    }

    /**
     * Inserts a key-reference pair into the specified index.
     *
     * @param crateName the name of the crate
     * @param indexName the name of the index to insert into
     * @param indexEntry the {@link IndexEntry}
     *
     * @return boolean true if the insert was successful, false if not
     *
     * @throws DuplicateKeyException if the key already exists in a unique index
     * @throws IllegalArgumentException if the index does not exist
     */
    @SuppressWarnings("unchecked")
    public boolean insert(String crateName, String indexName, IndexEntry<?> indexEntry) throws DuplicateKeyException {
        BPTreeIndex<?> index = getIndex(crateName, indexName);
        if (index == null) {
            LOGGER.error(String.format("'%s' index not found for inserting key: '%s'", indexName, indexEntry.key()),
                    IllegalArgumentException.class.getName());
            throw new IllegalArgumentException(INDEX_NOT_FOUND + indexName);
        }
        return index.insert(indexEntry.key(), indexEntry.reference());
    }

    /**
     * Inserts a key-reference pair into the specified index.
     *
     * @param crateName the name of the crate
     * @param indexName the name of the index to insert into
     *
     * @return {@link List<PageOffsetReference>}
     *
     * @throws DuplicateKeyException if the key already exists in a unique index
     * @throws IllegalArgumentException if the index does not exist
     */
    public List<PageOffsetReference> search(String crateName, String indexName, Comparable<?> key) {
        BPTreeIndex<?> index = getIndex(crateName, indexName);
        if (index == null) {
            LOGGER.error(String.format("'%s' index not found for inserting key: '%s'", indexName, key),
                    IllegalArgumentException.class.getName());
            throw new IllegalArgumentException(INDEX_NOT_FOUND + indexName);
        }
        return index.search(key); // This needs to accept Comparable<?> in BPTreeIndex
    }

    /**
     * Searches the specified index for a key.
     *
     * @param crateName the name of the crate to search
     * @param indexName the name of the index to search
     * @param indexEntry the {@link IndexEntry}
     *
     * @return a list of page-offset references associated with the key
     *
     * @throws IllegalArgumentException if the index does not exist
     */
    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> List<PageOffsetReference> search(String crateName, String indexName, IndexEntry<T> indexEntry) {
        BPTreeIndex<T> index = (BPTreeIndex<T>) getIndex(crateName, indexName);
        if (index == null) {
            LOGGER.error(String.format("'%s' index not found for inserting key: '%s'", indexName, indexEntry.key()),
                    IllegalArgumentException.class.getName());
            throw new IllegalArgumentException(INDEX_NOT_FOUND + indexName);
        }
        return index.search(indexEntry.key());
    }

    /**
     * Performs a range search on the tree, retrieving all keys and their associated references
     * that fall within the specified range [lowerBound, upperBound], inclusive.
     *
     * @param crateName the name of the crate being searched
     * @param indexName the name of the index being searched
     * @param lowerBound the minimum key value (inclusive) for the range search
     * @param upperBound the maximum key value (inclusive) for the range search
     *
     * @return a map where each key within the range is associated with a list of {@link PageOffsetReference},
     *         or {@code null} if no keys fall within the specified range
     */
    @SuppressWarnings("unchecked")
    public Map<?, List<PageOffsetReference>> rangeSearch(String crateName, String indexName, Comparable<?> lowerBound, Comparable<?> upperBound) {
        BPTreeIndex<?> index = getIndex(crateName, indexName);
        if (index == null) {
            LOGGER.error(String.format("'%s' index not found for inserting keys: '%s' and '%s'", indexName, lowerBound, upperBound),
                    IllegalArgumentException.class.getName());
            throw new IllegalArgumentException(INDEX_NOT_FOUND + indexName);
        }
        return index.rangeSearch(lowerBound, upperBound);
    }

    /**
     * Get all indexes managed by the BPTreeIndexManager.
     *
     * @return {@link List<BPTreeIndex>}
     */
    public List<BPTreeIndex<?>> getAllIndexes() {
        List<BPTreeIndex<?>> allIndexes = new ArrayList<>();
        for (String crate : this.crateIndexes.keySet()) {
            allIndexes.addAll(this.crateIndexes.get(crate).values());
        }
        return allIndexes;
    }

    public List<BPTreeIndex<?>> getAllIndexesFromCrate(String crateName) {
        List<BPTreeIndex<?>> allIndexesFromCrate = new ArrayList<>();
        for (String crate : this.crateIndexes.keySet()) {
            if (crate.equals(crateName)) {
                allIndexesFromCrate.addAll(this.crateIndexes.get(crate).values());
                break;
            }
        }
        return allIndexesFromCrate;
    }

    /**
     * Returns the {@link BPTreeIndex} instance associated with the given Crate and Index.
     *
     * @param crateName the name of the Crate
     * @param indexName the name of the index
     *
     * @return the corresponding {@link BPTreeIndex}, or null if not found.
     *
     * @see Crate
     * @see BPTreeIndex
     */
    public BPTreeIndex<?> getIndex(String crateName, String indexName) {
        return this.crateIndexes.getOrDefault(crateName, Collections.emptyMap()).get(indexName);
    }
}
