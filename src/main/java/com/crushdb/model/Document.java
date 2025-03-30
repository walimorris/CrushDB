package com.crushdb.model;

import com.crushdb.storageengine.page.Page;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a single Document within the CrushDB database.
 *
 * <p>Each document is uniquely identified by a {@code documentId} and contains
 * key-value pairs representing structured data. Documents are stored in
 * {@link Page}s and serialized into a binary format for efficient storage and retrieval.</p>
 *
 * <h2>Document Structure:</h2>
 * A Document consists of:
 * <ul>
 *   <li><b>Metadata:</b> Includes a unique document ID and size information.</li>
 *   <li><b>Key-Value Fields:</b> Stores data in a simple key-value format.</li>
 * </ul>
 *
 * <h2>Document Storage Format:</h2>
 * Each document stored in a Page follows this structure:
 * <pre>
 * +------------+------------+--------------+--------------+--------------+--------------------+
 * | 8 bytes    | 8 bytes    | 4 bytes      | 4 bytes      | 1 byte        | Variable          |
 * | documentId | pageId     | decompressed | compressed   | deletedFlag   | documentContent   |
 * |            |            | size         | size         | (1=active,    | (compressed or    |
 * |            |            |              |              | 0=deleted)    | uncompressed)     |
 * +------------+------------+--------------+--------------+--------------+--------------------+
 * </pre>
 * <ul>
 *   <li><b>Document ID (8 bytes):</b> Unique identifier for the document.</li>
 *   <li><b>Decompressed Size (4 bytes):</b> Original document size before compression.</li>
 *   <li><b>Compressed Size (4 bytes):</b> Size after compression (0 if not compressed).</li>
 *   <li><b>Deleted Flag (1 byte):</b> Indicates whether the document is deleted (0 = active, 1 = deleted).</li>
 *   <li><b>Document Content (variable):</b> Serialized document data (compressed if applicable).</li>
 * </ul>
 *
 * <h2>Key-Value Format:</h2>
 * Document content is stored as UTF-8 encoded key-value pairs, where each pair follows:
 * <pre>
 * key1:value1;key2:value2;key3:value3;
 * </pre>
 * Each key-value pair is separated by a semicolon (`;`), and keys/values are delimited by a colon (`:`).
 *
 * <h2>Operations Supported:</h2>
 * The Document class provides:
 * <ul>
 *   <li><b>Insertion:</b> Add new key-value pairs.</li>
 *   <li><b>Retrieval:</b> Access stored values using their keys.</li>
 *   <li><b>Serialization:</b> Convert a document into byte format for storage.</li>
 *   <li><b>Deserialization:</b> Reconstruct a document from stored bytes.</li>
 * </ul>
 *
 * <h2>Example Usage:</h2>
 * <pre>
 * Document doc = new Document(1);
 * doc.put("name", "Michael Jordan");
 * doc.put("profession", "Basketball");
 * doc.put("age", "62");
 *
 * byte[] serialized = doc.toBytes();  // Convert to binary format
 * Document reconstructed = Document.fromBytes(serialized);  // Restore from bytes
 * </pre>
 *
 * <p><b>Note:</b> This class is designed for use within CrushDB and assumes
 * an internal storage system that supports structured data storage.
 *
 * Documents do not yet store deleted-flag(this will be implemented). See
 * the Page class for Document storage format with delete-flag.
 * </p>
 *
 * TODO: reference to page to always reflect changes
 * TODO: documents should follow compression state for compression size validation
 * (if autoCompressOnInsert is on or off, the decompress value should reflect this)
 *
 * @author Wali Morris
 * @version 1.0
 */
public class Document {

    /**
     * Unique identifier for the document. Assigned upon creation and cannot be modified.
     */
    private final long documentId;

    /**
     * A unique identifier for a specific page. This value is immutable and
     * is used to distinctly reference a page.
     */
    private final long pageId;

    /**
     * Represents the size of a Document after decompression in bytes.
     * This variable stores the total length of the decompressed data.
     */
    private int decompressedSize;

    /**
     * Represents the size of a compressed Document in bytes.
     * Stores the byte count after compression is applied to the data.
     */
    private int compressedSize;

    /**
     * Stores the key-value pairs representing the document fields.
     * <p>Keys and values are stored as {@code String} objects, allowing flexible data storage.</p>
     */
    private final Map<String, BsonValue> fields;

    /**
     * The size of metadata stored with each document.
     * This includes:
     * - Document ID (8 bytes)
     * - Decompressed Document Size (4 bytes)
     * - Compressed Document Size (4 bytes)
     * - Deleted Flag size (1 byte)
     */
    public static final int DOCUMENT_METADATA_SIZE = 25;

    /**
     * Constructs a new document with a unique identifier.
     *
     * @param documentId The unique identifier for this document.
     */
    public Document(long documentId) {
        this.documentId = documentId;
        this.pageId = -1;
        this.decompressedSize = -1;
        this.compressedSize = -1;
        this.fields = new HashMap<>();
        this.fields.put("_id", BsonValue.ofLong(documentId));
    }

    /**
     * Constructor for a document that already belongs to a page.
     *
     * @param documentId The unique identifier for this document
     * @param pageId The ID of the page this document is stored in
     */
    public Document(long documentId, long pageId, int decompressedSize, int compressedSize) {
        this.documentId = documentId;
        this.pageId = pageId;
        this.decompressedSize = DOCUMENT_METADATA_SIZE + decompressedSize;
        this.compressedSize = DOCUMENT_METADATA_SIZE + compressedSize;
        this.fields = new HashMap<>();
    }

    /**
     * Returns the document in json like format. Does not include
     * {@code Note: Object or Arrays not currently supported}.
     * <br>
     * <p>
     *     Example result: {@code {_id:1,profession:Basketball,name:Jim,age:62}}
     * </p>
     * <br>
     * TODO: apply rules to document fields
     *
     * @return {@link String}
     */
    public String toString() {
        if (this.fields.isEmpty()) {
            return "{}";
        }
        if (this.fields.size() == 1) { // only id exist
            return String.format("{%s:%d}", "_id", this.fields.get("_id").asLong());
        }

        // get all the field pairs before building the string result
        StringBuilder fieldsStr = new StringBuilder();
        for (Map.Entry<String, BsonValue> fields : this.fields.entrySet()) {
            fieldsStr.append(fields.getKey()).append(":");
            BsonValue value = fields.getValue();
            switch (value.bsonType()) {
                case INT -> fieldsStr.append(value.asInteger()).append(";");
                case LONG -> fieldsStr.append(value.asLong()).append(";");
                case FLOAT -> fieldsStr.append(value.asFloat()).append(";");
                case DOUBLE -> fieldsStr.append(value.asDouble()).append(";");
                case BOOLEAN -> fieldsStr.append(value.asBoolean()).append(";");
                default -> fieldsStr.append(value.asString()).append(";");
            }
        }

        // fields will look as such a:1;b:2;c:3; to give structure and allow split
        // we shouldn't end fields content with a ';' so it must be removed before
        // processing the field data
        String content = fieldsStr.toString();
        if (content.endsWith(";")) {
            content = content.substring(0, content.length() - 1);
        }
        StringBuilder result = new StringBuilder();

        result.append("{");
        result.append("_id")
                .append(":")
                .append(this.fields.get("_id").asLong())
                .append(",");

        String[] pairs = content.split(";");
        for (String pair : pairs) {
            if (pair.isEmpty() || pair.contains("_id")) {
                continue;
            }
            String[] fieldPairs = pair.split(":");
            result.append(fieldPairs[0])
                    .append(":")
                    .append(fieldPairs[1])
                    .append(",");
        }
        result = new StringBuilder(result.substring(0, result.toString().length() - 1));
        result.append("}");
        return result.toString();
    }

    /**
     * Serializes this {@link Document} into a BSON-style binary format.
     *
     * @return A byte array representing the encoded document
     *
     * @see BsonEncoder
     */
    public byte[] toBytes() {
        return BsonEncoder.encode(this);
    }

    /**
     * Deserializes a BSON-style binary byte array into a {@link Document}.
     *
     * @param data The binary data to decode
     *
     * @return A reconstructed {@link Document} instance
     *
     * @see BsonDecoder
     */
    public static Document fromBytes(byte[] data) {
        return BsonDecoder.decode(data);
    }

    /**
     * Retrieves the internal map of fields for this document.
     *
     * @return A {@link Map} containing all key-value pairs in the document
     */
    public Map<String, BsonValue> getFields() {
        return this.fields;
    }

    /**
     * Inserts or updates a field in the document.
     *
     * <p>If the key already exists, the value will be updated.</p>
     *
     * @param key   The field name (cannot be {@code null})
     * @param value The field value (cannot be {@code null})
     *
     * @throws IllegalArgumentException if either key or value is null
     */
    public void put(String key, BsonValue value) {
        this.fields.put(key, value);
    }

    /**
     * Get {@link Document} String value utilizing {@link BsonValue#asString()}.
     *
     * @param key {@link String}
     *
     * @return {@link String}
     */
    public String getString(String key) {
        return this.fields.get(key).asString();
    }

    /**
     * Get {@link Document} Integer value utilizing {@link BsonValue#asInteger()}.
     *
     * @param key {@link String}
     *
     * @return int
     */
    public int getInt(String key) {
        return this.fields.get(key).asInteger();
    }

    /**
     * Get {@link Document} long value utilizing {@link BsonValue#asLong()}.
     *
     * @param key {@link String}
     *
     * @return long
     */
    public long getLong(String key) {
        return this.fields.get(key).asLong();
    }

    /**
     * Get {@link Document} Float value utilizing {@link BsonValue#asFloat()}.
     *
     * @param key {@link String}
     *
     * @return float
     */
    public float getFloat(String key) {
        return this.fields.get(key).asFloat();
    }

    /**
     * Get {@link Document} Double value utilizing {@link BsonValue#asDouble()}.
     *
     * @param key {@link String}
     *
     * @return double
     */
    public double getDouble(String key) {
        return this.fields.get(key).asDouble();
    }

    /**
     * Get {@link Document} Boolean value utilizing {@link BsonValue#asBoolean()}.
     *
     * @param key {@link String}
     *
     * @return boolean
     */
    public boolean getBoolean(String key) {
        return this.fields.get(key).asBoolean();
    }

    /**
     * Returns the unique document ID associated with this document.
     *
     * @return The document ID
     */
    public long getDocumentId() {
        return this.documentId;
    }

    /**
     * Returns the pageId associated with this document.
     *
     * @return the document Page ID
     */
    public long getPageId() {
        return this.pageId;
    }

    /**
     * Get {@link Document} decompressed size.
     *
     * @return int
     */
    public int getDecompressedSize() {
        return this.decompressedSize;
    }

    /**
     * Get {@link Document} compressed size.
     *
     * @return int
     */
    public int getCompressedSize() {
        return this.compressedSize;
    }
}
