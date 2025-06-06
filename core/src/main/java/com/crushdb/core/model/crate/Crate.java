package com.crushdb.core.model.crate;

import com.crushdb.core.index.BPTreeIndex;
import com.crushdb.core.index.btree.SortOrder;
import com.crushdb.core.model.document.BsonType;
import com.crushdb.core.model.document.BsonValue;
import com.crushdb.core.model.document.Document;
import com.crushdb.core.storageengine.StorageEngine;
import com.crushdb.core.bootstrap.ConfigManager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Crate {
    private final String name;
    private final StorageEngine storageEngine;
    private final Set<BPTreeIndex<?>> crateIndexes;
    private final String fileName;

    private final String CRATE_DIRECTORY = ConfigManager.CRATES_DIR;

    // TODO: Need disk persistence across restarts
    public Crate(String name, StorageEngine storageEngine) {
        this.name = name;
        this.storageEngine = storageEngine;
        this.crateIndexes = new HashSet<>(); // I want to set max size
        this.fileName = name + ".crate";
    }

    /**
     * Inserts a document into the storage engine and associates it with the relevant indexes.
     * The method checks the document's fields against the crate's indexes and adds the document
     * to the applicable indexes in the storage engine. If no relevant index is found, the document
     * will not be inserted. At the least, a document will be added to the document_id index.
     *
     * @param document the document to be inserted
     * @return the inserted document if it was successfully added to relevant indexes, or null if no indexes match
     */
    public Document insert(Document document) {
        // get the index
        // what if document Id is empty?
        List<BPTreeIndex<?>> indexes = new ArrayList<>();
        for (BPTreeIndex<?> crateIndex : crateIndexes) {
            if (document.getFields().containsKey(crateIndex.getFieldName())) {
                indexes.add(crateIndex);
            }
        }
        // the storage engine handles adding documents to the respective index, however
        // crates store the indexes relevant to each crate
        if (!indexes.isEmpty()) {
            return storageEngine.insert(this.name, document, indexes);
        }
        return storageEngine.insert(this.name, document);
    }

    /**
     * Searches for documents in the storage engine that match the specified field and value.
     * The search process prioritizes indexed fields for efficiency, but will perform a full
     * scan if no relevant index is found. Scans can be very expensive.
     *
     * @param field the name of the field to search for
     * @param value the value of the field to match
     * @return a list of documents that match the specified field and value
     * // TODO: we may need to hold single indexes and compound indexes in separate sets
     * // TODO: single indexes are only supported right now
     */
    public List<Document> find(String field, BsonValue value) {
        // we can delegate which documents to return to the QueryHandler based on the query.
        // in this layer, we just want to get the most relevant documents
        List<Document> documents = new ArrayList<>();
        int hit = 0;
        for (BPTreeIndex<?> index : this.crateIndexes) {
            // there can be different options here:
            // single index (i.e. make_index)
            // compound index (i.e. make_model_index or model_make_index)
            // in either case, if we're just searching on the field, the
            // index will be found, but we want the most optimal index to use
            // in this case, it's a single field index and we want that, if it
            // is the second field in a compound index, it'll be expensive.
            // however, if it was the left most field, that's okay.
            // "make_index" -> [make], [index], "make_model_index" -> [make], [model], [index]
            String[] parts = index.getIndexName().split("_");
            if (parts.length == 2) { // single indexed field, includes "index"
                if (index.getFieldName().equals(field) && parts[0].equals(field)) {
                    documents.addAll(storageEngine.find(index.getCrateName(), index, value));
                    hit++;
                    break;
                }
            }
//            if (parts.length == 3) { // compound indexed fields, includes "index"
//                if (index.getFieldName().startsWith(field)) {
//                    documents.addAll(storageEngine.find(index.getCrateName(), index, field, value));
//                    hit++;
//                    break;
//                }
//            }
        }
        // no index hits - run a scan!
        if (hit == 0) {
            documents.addAll(storageEngine.scan(this.name, field, value));
        }
        return documents;
    }

    /**
     * Creates an index for a specified field in the storage engine.
     * This method delegates the index creation to the underlying storage engine.
     *
     * @param bsonType the index bson type
     * @param indexName the name of the index to be created, used for identification
     * @param fieldName the name of the field on which the index will be created
     * @param unique a flag indicating whether the index should enforce uniqueness for the indexed field
     * @param order the order of the underlying tree
     * @param sortOrder the sort order for the index, either ascending or descending, represented by {@link SortOrder}
     */
    public <T extends Comparable<T>> void createIndex(BsonType bsonType, String indexName, String fieldName, boolean unique, int order, SortOrder sortOrder) {
        Class<? extends Comparable<?>> clazz = bsonType.getJavaType();
        BPTreeIndex<T> index = storageEngine.createIndex(bsonType, this.name, indexName, fieldName, unique, order, sortOrder);
        this.crateIndexes.add(index);
    }

    /**
     * Retrieves the index corresponding to the specified field name, if one exists.
     * The method searches through the crate's indexes and returns an {@link Optional}
     * containing the index if a match is found, or an empty {@code Optional} otherwise.
     *
     * @param fieldName the name of the field for which the index is being retrieved
     * @return an {@link Optional} containing the {@link BPTreeIndex} if it exists,
     *         or an empty {@code Optional} if no matching index is found
     */
    public Optional<BPTreeIndex<?>> getIndex(String fieldName) {
        for (BPTreeIndex<?> index : this.crateIndexes) {
            if (index.getFieldName().equals(fieldName)) {
                return Optional.of(index);
            }
        }
        return Optional.empty();
    }

    /**
     * Get Crate name.
     *
     * @return {@link String}
     */
    public String getName() {
        return this.name;
    }

    public void serialize(String path) {
        try (BufferedWriter writer = Files.newBufferedWriter(Path.of(path))) {
            writer.write("name=" + name);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Crate: " + this.name, e);
        }
    }

    public static Crate deserialize(Path path, StorageEngine storageEngine) {
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            String name = null;

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("name=")) {
                    name = line.substring("name=".length());
                }
            }
            if (name == null) {
                throw new IllegalArgumentException("Invalid Crate file: missing name");
            }
            return new Crate(name, storageEngine);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize Crate from: " + path, e);
        }
    }
}
