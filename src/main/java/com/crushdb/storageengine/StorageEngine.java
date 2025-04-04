package com.crushdb.storageengine;

import com.crushdb.index.BPTreeIndex;
import com.crushdb.index.BPTreeIndexManager;
import com.crushdb.index.IndexEntry;
import com.crushdb.index.IndexEntryBuilder;
import com.crushdb.index.btree.PageOffsetReference;
import com.crushdb.index.btree.SortOrder;
import com.crushdb.model.document.BsonType;
import com.crushdb.model.document.BsonValue;
import com.crushdb.model.document.Document;
import com.crushdb.storageengine.page.PageManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StorageEngine {
    private final PageManager pageManager;
    private final BPTreeIndexManager indexManager;

    public StorageEngine(PageManager pageManager, BPTreeIndexManager indexManager) {
        this.pageManager = pageManager;
        this.indexManager = indexManager;
    }

    /**
     * Inserts a document into the storage engine and indexes it using the specified indexes.
     *
     * The method attempts to insert the document into the underlying storage through the page
     * manager. If the insertion is successful, it proceeds to index the document using the list
     * of available indexes. Each index is updated with the fields of the inserted document
     * that match the corresponding index name.
     *
     * @param document the document to be inserted; must not be null
     * @param indexes a list of BPTreeIndex objects to index the document; must not be null
     *
     * @return the inserted document if the operation is successful, or null if the insertion fails
     *
     * @see PageManager
     * @see BPTreeIndexManager
     */
    public Document insert(Document document, List<BPTreeIndex<?>> indexes) {
        Document insertedDocument = pageManager.insertDocument(document);
        if (insertedDocument != null) {
            // index the document - any fields that matches an index name will be indexed
            // so even _id will be indexed on the _id_index
            for (BPTreeIndex<?> index : indexes) {
                IndexEntry<?> entry = IndexEntryBuilder.fromDocument(document, index);
                indexManager.insert(index.getCrateName(), index.getIndexName(), entry);
            }
            return insertedDocument;
        }
        return null;
    }

    /**
     * Searches for documents in the storage engine based on the specified field, index, and value.
     * If a valid index is provided, this method leverages the index to perform the search more
     * efficiently. If the value is null, an exception is thrown.
     *
     * @param crateName the name of the crate
     * @param field the name of the field to search on; must not be null or empty
     * @param index the BPTreeIndex object to use for searching; can be null if no index is available
     * @param value the value to search for within the specified field; must not be null
     *
     * @return a list of documents that match the specified criteria, or null if no index is provided
     *
     * @throws IllegalArgumentException if the value is null
     */
    public List<Document> find(String crateName, String field, BPTreeIndex<?> index, BsonValue value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException("Value is empty for search on crate: " + crateName + ", on field: " + field);
        }
        // use the index if it exists
        if (index != null) {
            return findWithTypedKey(index, value);
        }
        return null;
    }

    /**
     * Performs a range-based search on a specified field within a crate using the provided tree index.
     * If a valid index is provided, this method retrieves all documents where the field's value is within
     * the specified lower and upper bounds. Both bounds are inclusive.
     *
     * @param crateName the name of the crate to search within; must not be null or empty
     * @param field the name of the field to search on; must not be null or empty
     * @param index the BPTreeIndex object to use for searching; can be null if no index is available
     * @param lowerBound the lower bound of the range; must not be null
     * @param upperBound the upper bound of the range; must not be null
     *
     * @return a list of documents that match the specified range criteria, or null if no index is provided
     *
     * @throws IllegalArgumentException if either lowerBound or upperBound is null
     */
    public List<Document> rangeFind(String crateName, String field, BPTreeIndex<?> index, BsonValue lowerBound, BsonValue upperBound) throws IllegalArgumentException {
        if (lowerBound == null || upperBound == null) {
            throw new IllegalArgumentException("Value is empty for search on crate: " + crateName + ", on field: " + field);
        }
        if (index != null) {
            return rangeFindWithTypedKey(index, lowerBound, upperBound);
        }
        return null;
    }

    /**
     * Scans all in-memory pages to retrieve a list of documents matching the specified field and value.
     * This method filters documents based on the specified field name and value.
     *
     * @param fieldName the name of the field to filter documents by
     * @param value the value to match against the specified field; must not be null
     *
     * @return a list of documents that match the specified field and value, or an empty list if no matches are found
     */
    public List<Document> scan(String fieldName, BsonValue value) {
        return this.pageManager.getAllInMemoryPages()
                .stream()
                .flatMap(page -> page.getDocuments().stream())
                .filter(doc -> value.equals(doc.get(fieldName)))
                .toList();
        // TODO: add persisted documents too. This is SO EXPENSIVE >>> How can i make this
        // TODO: more proficient?
    }

    public <T extends Comparable<T>> BPTreeIndex<T> createIndex(BsonType bsonType, String crateName, String indexName, String fieldName, boolean unique, int order, SortOrder sortOrder) {
        return indexManager.createIndex(bsonType, crateName, indexName, fieldName, unique, order, sortOrder);
        // TODO: order should probably come from the index itself, from the tree.
    }

    private List<Document> findWithTypedKey(BPTreeIndex<?> rawIndex, BsonValue bsonValue) {
        BsonType expectedType = rawIndex.bsonType();

        // does the value match the expected type?
        if (!bsonValue.bsonType().equals(expectedType)) {
            throw new IllegalArgumentException("BsonValue type does not match index key type. Expected: "
                    + expectedType + ", but got: " + bsonValue.bsonType());
        }

        // expected typed key
        Comparable<?> key = bsonValue.toJavaValue();

        // utilize index manager using the raw types
        List<PageOffsetReference> refs = indexManager.search(rawIndex.getCrateName(), rawIndex.getIndexName(), key);

        return refs.stream()
                .map(this.pageManager::retrieveDocument)
                .toList();
    }

    private List<Document> rangeFindWithTypedKey(BPTreeIndex<?> rawIndex, BsonValue lowerBound, BsonValue upperBound) {
        BsonType expectedType = rawIndex.bsonType();

        // does the value match the expected type?
        if (!lowerBound.bsonType().equals(expectedType)) {
            throw new IllegalArgumentException("Lower bound BsonValue type does not match index key type. Expected: "
                    + expectedType + ", but got: " + lowerBound.bsonType());
        }
        if (!upperBound.bsonType().equals(expectedType)) {
            throw new IllegalArgumentException("Upper bound BsonValue type does not match index key type. Expected: "
                    + expectedType + ", but got: " + upperBound.bsonType());
        }

        // expected typed keys
        Comparable<?> lowerBoundKey = lowerBound.toJavaValue();
        Comparable<?> upperBoundKey = upperBound.toJavaValue();

        // ranged search return a map (key, references). In this case we must get all the references for each key
        // call the page manager to get the list of retrieved documents and add all documents to the result
        List<Document> results = new ArrayList<>();
        Map<?, List<PageOffsetReference>> refs = indexManager.rangeSearch(rawIndex.getCrateName(), rawIndex.getIndexName(), lowerBoundKey, upperBoundKey);
        for (Object key : refs.keySet()) {
            List<PageOffsetReference> pageOffsetReferences = refs.get(key);
            List<Document> documents = pageOffsetReferences.stream()
                    .map(this.pageManager::retrieveDocument)
                    .toList();
            results.addAll(documents);
        }
        return results;
    }
}
