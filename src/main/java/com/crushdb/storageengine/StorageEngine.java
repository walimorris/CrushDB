package com.crushdb.storageengine;

import com.crushdb.index.BPTreeIndex;
import com.crushdb.index.BPTreeIndexManager;
import com.crushdb.index.IndexEntry;
import com.crushdb.index.IndexEntryBuilder;
import com.crushdb.index.btree.PageOffsetReference;
import com.crushdb.model.BsonValue;
import com.crushdb.model.Document;
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
     * Inserts a given document into the storage engine and indexes its fields if applicable.
     *
     * This method first adds the document using the page manager and, if successful,
     * iterates through all available indexes to check if any field in the document
     * matches an index. If a match is found, the document is inserted into the corresponding index.
     *
     * @param document the document to be inserted into the storage engine
     *
     * @return the inserted document if successful, or null if the insertion fails
     *
     * @see PageManager
     * @see BPTreeIndexManager
     */
    public Document insert(Document document) {
        Document insertedDocument = pageManager.insertDocument(document);
        if (insertedDocument != null) {
            // index the document - any fields that matches an index name will be indexed
            // so even _id will be indexed on the _id_index
            for (BPTreeIndex<?> index : this.indexManager.getIndexes().values()) {
                if (insertedDocument.getFields().containsKey(index.getFieldName())) {
                    insertIntoIndex(index, insertedDocument);
                }
            }
            return insertedDocument;
        }
        return null;
    }

    /**
     * Retrieves a list of documents from the storage engine that matches the specified field and value.
     *
     * The method attempts to use an index for the given field. If an index exists, it performs a search using
     * the index for more efficient document retrieval. If no index is available, it falls back to scanning
     * all documents, which is awfully expensive.
     *
     * @param field the name of the field to search for in the documents
     * @param value the value to match against the specified field; must not be null
     *
     * @return a list of documents that match the specified field and value, or an empty list if no matches are found
     * @throws IllegalArgumentException if the provided value is null
     *
     */
    public List<Document> find(String field, BsonValue value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException("Value is empty for search on: " + field);
        }
        BPTreeIndex<?> index = this.indexManager.getIndex(field);
        // use the index if it exists
        if (index != null) {
            List<PageOffsetReference> refs = searchTyped(index, value);
            return refs.stream().map(this.pageManager::retrieveDocument)
                    .collect(Collectors.toList());
        } else {
            // run a full scan over all the documents - omg
            return scan(field, value);
        }
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
    private List<Document> scan(String fieldName, BsonValue value) {
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
}
