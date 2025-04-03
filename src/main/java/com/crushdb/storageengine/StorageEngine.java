package com.crushdb.storageengine;

import com.crushdb.index.BPTreeIndex;
import com.crushdb.index.BPTreeIndexManager;
import com.crushdb.index.IndexEntry;
import com.crushdb.index.IndexEntryBuilder;
import com.crushdb.index.btree.PageOffsetReference;
import com.crushdb.index.btree.SortOrder;
import com.crushdb.model.document.BsonValue;
import com.crushdb.model.document.Document;
import com.crushdb.storageengine.page.PageManager;

import java.util.List;
import java.util.stream.Collectors;

public class StorageEngine {
    private final PageManager pageManager;
    private final BPTreeIndexManager<?> indexManager;

    public StorageEngine(PageManager pageManager, BPTreeIndexManager<?> indexManager) {
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
                insertIntoIndex(index, insertedDocument);
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
     * @param field the name of the field to search on; must not be null or empty
     * @param index the BPTreeIndex object to use for searching; can be null if no index is available
     * @param value the value to search for within the specified field; must not be null
     *
     * @return a list of documents that match the specified criteria, or null if no index is provided
     *
     * @throws IllegalArgumentException if the value is null
     */
    public List<Document> find(String field, BPTreeIndex<?> index, BsonValue value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException("Value is empty for search on: " + field);
        }
        // use the index if it exists
        if (index != null) {
            List<PageOffsetReference> refs = searchTyped(index, value);
            return refs.stream().map(this.pageManager::retrieveDocument)
                    .collect(Collectors.toList());
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

    @SuppressWarnings("unchecked")
    private <T extends Comparable<T>> List<PageOffsetReference> searchTyped(BPTreeIndex<T> index, BsonValue value) {
        T typedKey = (T) value.toJavaValue();
        return index.search(typedKey);
    }

    @SuppressWarnings("unchecked")
    private <T extends Comparable<T>> void insertIntoIndex(BPTreeIndex<?> rawIndex, Document doc) {
        BPTreeIndex<T> index = (BPTreeIndex<T>) rawIndex;
        IndexEntry<T> entry = IndexEntryBuilder.fromDocument(doc, index);
        if (entry != null) {
            index.insert(entry);
        }
    }

    public void createIndex(String indexName, String fieldName, boolean unique, int order, SortOrder sortOrder) {
        indexManager.createIndex(indexName, fieldName, unique, order, sortOrder);
        // TODO: order should probably come from the index itself, from the tree.
    }
}
