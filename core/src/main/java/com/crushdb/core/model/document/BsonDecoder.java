package com.crushdb.core.model.document;

import com.crushdb.core.logger.CrushDBLogger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.crushdb.core.storageengine.page.Page.INACTIVE;

/**
 * Responsible for decoding a byte array into a {@link Document}
 * using the BSON-style binary format defined by CrushDB.
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
 * <p>The decoder interprets binary data in a self-describing format, where
 * each key-value pair includes metadata about its length and type. This allows
 * documents to be decoded without requiring an external schema definition.
 *
 * <p>The binary format is compatible with the structure produced by {@link BsonEncoder}
 * and is aligned with CrushDB's page storage model.
 *
 * @see BsonType
 * @see BsonValue
 * @see BsonEncoder
 */
public class BsonDecoder {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(BsonDecoder.class);

    /**
     * Decodes a BSON-style binary byte array into a {@link Document} instance.
     *
     * <p><b>Decoding Process:</b></p>
     * <ol>
     *   <li>Reads document metadata (documentId, pageId, decompressedSize, compressedSize, deleted flag).</li>
     *   <li>If the deleted flag indicates a tombstone, the method returns {@code null}.</li>
     *   <li>Reads the decompressed content section into a new buffer.</li>
     *   <li>Parses key-value pairs in the following format:
     *     <ul>
     *       <li><b>Key Length (2 bytes):</b> Length of the field name</li>
     *       <li><b>Key:</b> UTF-8 encoded field name</li>
     *       <li><b>Type Tag (1 byte):</b> BSON type of the value</li>
     *       <li><b>Value:</b> Parsed based on the BSON type
     *         <ul>
     *           <li>STRING: 2-byte length + UTF-8 bytes</li>
     *           <li>INT: 4 bytes</li>
     *           <li>LONG: 8 bytes</li>
     *           <li>FLOAT: 4 bytes</li>
     *           <li>DOUBLE: 8 bytes</li>
     *           <li>BOOLEAN: 1 byte (0x01 = true, 0x00 = false)</li>
     *         </ul>
     *       </li>
     *     </ul>
     *   </li>
     *   <li>Populates a {@link Document} with the reconstructed fields and returns it.</li>
     * </ol>
     *
     * <h3>Constraints and Assumptions:</h3>
     * <ul>
     *   <li>Data must follow the binary structure defined by {@link BsonEncoder}.</li>
     *   <li>Documents are stored in big-endian byte order (Java default).</li>
     *   <li>Compressed documents must be decompressed prior to decoding.</li>
     * </ul>
     *
     * @param data The binary byte array representing a serialized document
     *
     * @return A reconstructed {@link Document} object, or {@code null} if the document is marked as deleted
     *
     * @throws IllegalArgumentException If the input byte array is null or empty
     * @throws IllegalStateException If the buffer does not contain enough bytes to match the declared size
     */
    public static Document decode(byte[] data) {

        ByteBuffer buffer = ByteBuffer.wrap(data);

        long documentId = buffer.getLong();
        long pageId = buffer.getLong();
        int dcs = buffer.getInt();
        int cs = buffer.getInt();
        byte df = buffer.get();

        if (df == INACTIVE) {
            LOGGER.error("Document ID" + documentId + "is a tombstone marked for deletion", null);
            return null;
        }

        byte[] contentBytes = new byte[dcs];
        buffer.get(contentBytes);
        ByteBuffer contentBuffer = ByteBuffer.wrap(contentBytes);

        Document doc = new Document(documentId, pageId, dcs, cs);

        while (contentBuffer.remaining() > 0) {
            short keyLength = contentBuffer.getShort();
            byte[] keyBytes = new byte[keyLength];
            contentBuffer.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            byte typeByte = contentBuffer.get();
            BsonType type = BsonType.fromByte(typeByte);

            BsonValue value;
            switch (type) {
                case STRING -> {
                    short len = contentBuffer.getShort();
                    byte[] strBytes = new byte[len];
                    contentBuffer.get(strBytes);
                    value = BsonValue.ofString(new String(strBytes, StandardCharsets.UTF_8));
                }
                case INT -> value = BsonValue.ofInteger(contentBuffer.getInt());
                case LONG -> value = BsonValue.ofLong(contentBuffer.getLong());
                case FLOAT -> value = BsonValue.ofFloat(contentBuffer.getFloat());
                case DOUBLE -> value = BsonValue.ofDouble(contentBuffer.getDouble());
                case BOOLEAN -> value = BsonValue.ofBoolean(contentBuffer.get() == 0x01);
                default -> throw new IllegalStateException("Unsupported BsonType: " + type);
            }
            doc.put(key, value);
        }

        return doc;
    }
}
