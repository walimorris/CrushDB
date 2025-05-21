package com.crushdb.core.storageengine;

import com.crushdb.core.index.BPTreeIndex;
import com.crushdb.core.index.BPTreeIndexManager;
import com.crushdb.core.index.IndexEntry;
import com.crushdb.core.index.IndexEntryBuilder;
import com.crushdb.core.index.btree.PageOffsetReference;
import com.crushdb.core.index.btree.SortOrder;
import com.crushdb.core.logger.CrushDBLogger;
import com.crushdb.core.model.document.BsonType;
import com.crushdb.core.model.document.BsonValue;
import com.crushdb.core.model.document.Document;
import com.crushdb.core.storageengine.journal.JournalEntry;
import com.crushdb.core.storageengine.journal.JournalManager;
import com.crushdb.core.storageengine.page.PageManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StorageEngine {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(StorageEngine.class);

    private final PageManager pageManager;
    private final BPTreeIndexManager indexManager;
    private final JournalManager journalManager;

    public StorageEngine(PageManager pageManager, BPTreeIndexManager indexManager, JournalManager journalManager) {
        this.pageManager = pageManager;
        this.indexManager = indexManager;
        this.journalManager = journalManager;
    }

    /**
     * Inserts a document into the storage engine and indexes it using the specified indexes.
     * <p>
     * The method attempts to insert the document into the underlying storage through the page
     * manager. If the insertion is successful, it proceeds to index the document using the list
     * of available indexes. Each index is updated with the fields of the inserted document
     * that match the corresponding index name.
     *
     * @param document the document to be inserted; must not be null
     * @return the inserted document if the operation is successful, or null if the insertion fails
     * @see PageManager
     * @see BPTreeIndexManager
     */
    public Document insert(String crateName, Document document) {
        // append to journal
        this.journalManager.append(new JournalEntry(System.currentTimeMillis(), JournalEntry.OperationType.WRITE, crateName, document.getDocumentId()));
        Document insertedDocument = pageManager.insertDocument(document);
        if (insertedDocument != null) {
            // index the document - any fields that matches an index name will be indexed
            // so even _id will be indexed on the _id_index
            // none indexed document are only added to the page - accessible during expensive scans
            for (BPTreeIndex<?> index : indexManager.getAllIndexesFromCrate(crateName)) {
                IndexEntry<?> entry = IndexEntryBuilder.fromDocument(document, index);
                indexManager.insert(index.getCrateName(), index.getIndexName(), entry);
            }
            return insertedDocument;
        }
        return null;
    }

    public Document insert(String crateName, Document document, List<BPTreeIndex<?>> indexes) {
        // journal append
        this.journalManager.append(new JournalEntry(System.currentTimeMillis(), JournalEntry.OperationType.WRITE, crateName, document.getDocumentId()));
        Document insertedDocument = pageManager.insertDocument(document);
        for (BPTreeIndex<?> index: indexes) {
            IndexEntry<?> entry = IndexEntryBuilder.fromDocument(document, index);
            indexManager.insert(crateName, index.getIndexName(), entry);
        }
        return insertedDocument;
    }

    /**
     * Searches for documents in the storage engine based on the specified field, index, and value.
     * If a valid index is provided, this method leverages the index to perform the search more
     * efficiently. If the value is null, an exception is thrown.
     *
     * @param crateName the name of the crate
     * @param index     the BPTreeIndex object to use for searching; can be null if no index is available
     * @param value     the value to search for within the specified field; must not be null
     * @return a list of documents that match the specified criteria, or null if no index is provided
     * @throws IllegalArgumentException if the value is null
     *                                  <p>
     *                                  TODO: validate that order is kept (sort order - descending/ascending) we do not want to conduct another
     *                                  TODO: sort on the results
     */
    public List<Document> find(String crateName, BPTreeIndex<?> index, BsonValue value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException("Value is empty for search on crate: " + crateName + ", on value: " + value);
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
     * @param crateName  the name of the crate to search within; must not be null or empty
     * @param index      the BPTreeIndex object to use for searching; can be null if no index is available
     * @param lowerBound the lower bound of the range; must not be null
     * @param upperBound the upper bound of the range; must not be null
     * @return a list of documents that match the specified range criteria, or null if no index is provided
     * @throws IllegalArgumentException if either lowerBound or upperBound is null
     */
    public List<Document> rangeFind(String crateName, BPTreeIndex<?> index, BsonValue lowerBound, BsonValue upperBound) throws IllegalArgumentException {
        if (lowerBound == null || upperBound == null) {
            throw new IllegalArgumentException("Value is empty for search on crate: " + crateName + ", on field: " + index.getFieldName());
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
     * @param value     the value to match against the specified field; must not be null
     * @return a list of documents that match the specified field and value, or an empty list if no matches are found
     */
    public List<Document> scan(String crateName, String fieldName, BsonValue value) {
        LOGGER.info(String.format("An expensive scan is being conducted on field '%s' in crate: '%s', recommend creating index",
                fieldName, crateName), null);
        return this.pageManager.getAllInMemoryPages()
                .stream()
                .flatMap(page -> page.getDocuments().stream())
                .filter(doc -> value.equals(doc.get(fieldName)))
                .toList();
        // TODO: add persisted documents too. This is SO EXPENSIVE >>> How can i make this
        // TODO: more proficient?
        // TODO: add some metadata in results - was it a scan, indexed search, etc. for query engine/planner
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

    public JournalManager getJournalManager() {
        return this.journalManager;
    }

    public PageManager getPageManager() {
        return this.pageManager;
    }

    public BPTreeIndexManager getIndexManager() {
        return this.indexManager;
    }
}
