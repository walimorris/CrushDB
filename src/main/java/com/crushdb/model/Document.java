package com.crushdb.model;

import com.crushdb.storageengine.page.Page;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static com.crushdb.storageengine.page.Page.INACTIVE;

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

    private long pageId;

    private int decompressedSize;

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
     * @param documentId The unique identifier for this document.
     * @param pageId The ID of the page this document is stored in.
     */
    public Document(long documentId, long pageId, int decompressedSize, int compressedSize) {
        this.documentId = documentId;
        this.pageId = pageId;
        this.decompressedSize = DOCUMENT_METADATA_SIZE + decompressedSize;
        this.compressedSize = DOCUMENT_METADATA_SIZE + compressedSize;
        this.fields = new HashMap<>();
    }

    /**
     * Returns the unique document ID associated with this document.
     *
     * @return The document ID.
     */
    public long getDocumentId() {
        return this.documentId;
    }

    /**
     * Returns the pageId associated with this document.
     *
     * @return the document Page ID.
     */
    public long getPageId() {
        return this.pageId;
    }

    public int getDecompressedSize() {
        return this.decompressedSize;
    }

    public int getCompressedSize() {
        return this.compressedSize;
    }

    /**
     * Retrieves the internal map of fields for this document.
     *
     * @return A {@link Map} containing all key-value pairs in the document.
     */
    public Map<String, BsonValue> getFields() {
        return this.fields;
    }

    /**
     * Inserts or updates a field in the document.
     *
     * <p>If the key already exists, the value will be updated.</p>
     *
     * @param key   The field name (cannot be {@code null}).
     * @param value The field value (cannot be {@code null}).
     * @throws IllegalArgumentException if either key or value is null.
     */
    public void put(String key, BsonValue value) {
        this.fields.put(key, value);
    }

    /**
     * Retrieves the value associated with the specified key.
     *
     * @param key The field name to look up.
     * @return The field value, or {@code null} if the key does not exist.
     */
    public BsonValue get(String key) {
        return this.fields.get(key);
    }

    /**
     * Serializes the {@link Document} into a byte array representation.
     *
     * <p><b>Serialization Process:</b></p>
     * <ol>
     *   <li>Converts document fields (key-value pairs) into a UTF-8 encoded string.</li>
     *   <li>Uses a delimiter (';') to separate fields.</li>
     *   <li>Encodes the formatted string into a byte array.</li>
     *   <li>Stores metadata (documentId, document size) using a {@link ByteBuffer}.</li>
     *   <li>Combines metadata and document content into the final byte array.</li>
     * </ol>
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
     * <p><b>Example of Stored Content:</b></p>
     * <pre>
     * profession:Basketball;name:Michael Jordan;age:62;
     * </pre>
     *
     * <h2>Notes & Constraints:</h2>
     * <ul>
     *   <li>Keys and values are stored as UTF-8 strings.</li>
     *   <li>Each key-value pair follows the format: {@code key:value;}</li>
     *   <li>The final byte array includes metadata for efficient retrieval.</li>
     *   <li>Trailing semicolons (';') indicate the end of a pair but are not necessary for parsing.</li>
     * </ul>
     *
     * @return A byte array containing the serialized document, including metadata.
     */
    public byte[] toBytes() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, BsonValue> entry : fields.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Deserializes a byte array into a {@link Document}.
     *
     * <p><b>Deserialization Process:</b></p>
     * <ol>
     *   <li>Reads the metadata fields:
     *       <ul>
     *           <li><b>Document ID (8 bytes):</b> Unique identifier for the document.</li>
     *           <li><b>Decompressed Size (4 bytes):</b> The expected size of the document content.</li>
     *           <li><b>Compressed Size (4 bytes):</b> The actual size if the document was compressed (0 if uncompressed).</li>
     *       </ul>
     *   </li>
     *   <li>Verifies that the buffer contains enough remaining bytes to store the document content.</li>
     *   <li>Extracts the document content as a UTF-8 encoded string.</li>
     *   <li>Removes any trailing delimiter (';') if present.</li>
     *   <li>Parses the key-value pairs using the format: {@code key:value;}</li>
     *   <li>Constructs and returns a {@link Document} object with the parsed key-value pairs.</li>
     * </ol>
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
     * <p><b>Example of Stored Content:</b></p>
     * <pre>
     * profession:Basketball;name:Michael Jordan;age:62;
     * </pre>
     *
     * <h2>Assumptions & Constraints:</h2>
     * <ul>
     *   <li>Documents are stored in Big-Endian byte order (Java default).</li>
     *   <li>Compressed documents should be decompressed before calling this method.</li>
     *   <li>Trailing delimiters (e.g., ';') are removed before parsing.</li>
     * </ul>
     *
     * @param data The byte array representing the serialized document.
     * @return A {@link Document} instance reconstructed from the byte array.
     * @throws IllegalArgumentException If the input byte array is null or empty.
     * @throws IllegalStateException If the buffer does not contain enough data.
     */
    public static Document fromBytes(byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Invalid byte array: data is null or empty");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);

        long documentId = buffer.getLong();
        long pageId = buffer.getLong();
        int dcs = buffer.getInt();
        int cs = buffer.getInt();
        byte df = buffer.get();

        if (df == INACTIVE) {
            System.err.println("ERROR: Document ID " + documentId + " is a tombstone marked for deletion.");
            return null;
        }

        // Ensure there's enough data in the buffer
        if (buffer.remaining() < dcs) {
            throw new IllegalStateException("ERROR: Buffer does not have enough remaining bytes! " +
                    "Expected: " + dcs + ", Available: " + buffer.remaining());
        }
        byte[] contentBytes = new byte[dcs];
        buffer.get(contentBytes);

        String content = new String(contentBytes, StandardCharsets.UTF_8).trim();
        if (content.endsWith(";")) {
            content = content.substring(0, content.length() - 1);
        }

        // Parse document fields
        // TODO: document isn't being read correctly with single pair ("name":"same")
        Document doc = new Document(documentId, pageId, dcs, cs);
        String[] pairs = content.split(";");
        for (String pair : pairs) {
            if (pair.isEmpty()) continue;

            int colonIndex = pair.indexOf(':');
            if (colonIndex > 0) {
                String key = pair.substring(0, colonIndex).trim();
                BsonValue value = BsonValue.ofString(pair.substring(colonIndex + 1).trim());
                doc.put(key, value);
            }
        }
        return doc;
    }

    /**
     * Returns the document in json like format. Does not include
     * Object or Array like format conventions.
     *
     * TODO: apply rules to document fields
     *
     * @return {@link String}
     */
    public String toString() {
        if (this.fields.isEmpty()) {
            return "{}";
        }
        if (this.fields.size() == 1) { // only id exist
            return String.format("{%s:%s}", "_id", this.fields.get("_id"));
        }
        StringBuilder fieldsStr = new StringBuilder();
        for (Map.Entry<String, BsonValue> fields : this.fields.entrySet()) {
            fieldsStr.append(fields.getKey()).append(":").append(fields.getValue()).append(";");
        }
        String content = fieldsStr.toString();
        if (content.endsWith(";")) {
            content = content.substring(0, content.length() - 1);
        }
        StringBuilder result = new StringBuilder();

        result.append("{");
        result.append("_id")
                .append(":")
                .append(this.fields.get("_id"))
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
}
