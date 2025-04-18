package com.crushdb.core.model.document;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Responsible for encoding a {@link Document} into its binary representation
 * using a BSON-style serialization format.
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
 * <p>The encoding process stores each key-value pair as a binary sequence that includes:
 * <ul>
 *   <li><b>Key Length (2 bytes):</b> Length of the UTF-8 encoded key.</li>
 *   <li><b>Key:</b> UTF-8 encoded bytes representing the field name.</li>
 *   <li><b>BSON Type (1 byte):</b> A type tag that identifies the data type of the value.</li>
 *   <li><b>Value:</b> The binary-encoded value, format depends on the type (e.g. int, string, etc.).</li>
 * </ul>
 *
 * <p>This format enables self-describing documents where each field carries its type
 * and size information, allowing for efficient decoding without requiring a schema.
 *
 * <p>Note: The encoder currently assumes a maximum document size of 4KB, aligned with
 * the default page size in CrushDB.
 *
 * @see BsonType
 * @see BsonValue
 * @see BsonEncoder
 */
public class BsonEncoder {

    /**
     * Encodes the provided {@link Document} into a binary format suitable for storage in CrushDB pages.
     *
     * <p><b>Encoding Process:</b></p>
     * <ol>
     *   <li>Allocates a fixed-size byte buffer (4KB) to hold the serialized document.</li>
     *   <li>Iterates through all key-value pairs in the document.</li>
     *   <li>For each field:
     *     <ul>
     *       <li>Writes the key length (2 bytes) and UTF-8 encoded key bytes.</li>
     *       <li>Writes a 1-byte BSON type tag to identify the value type.</li>
     *       <li>Writes the value in its binary form:
     *         <ul>
     *           <li>String: 2 bytes for length, followed by UTF-8 bytes</li>
     *           <li>Integer: 4 bytes</li>
     *           <li>Long: 8 bytes</li>
     *           <li>Float: 4 bytes</li>
     *           <li>Double: 8 bytes</li>
     *           <li>Boolean: 1 byte (0x01 = true, 0x00 = false)</li>
     *         </ul>
     *       </li>
     *     </ul>
     *   </li>
     *   <li>Trims the buffer to the exact number of bytes written and returns the result.</li>
     * </ol>
     *
     * <h2>Constraints:</h2>
     * <ul>
     *   <li>The total encoded document size must not exceed 4096 bytes (4KB).</li>
     *   <li>UTF-8 encoding is used for both keys and string values.</li>
     *   <li>No compression or overflow page support is currently implemented.</li>
     * </ul>
     *
     * @param doc The {@link Document} to encode
     *
     * @return A byte array containing the encoded document in BSON-style binary format
     */
    public static byte[] encode(Document doc) {

        // TODO: make this dynamic in the future - dependent on configured page size. i.e. document should
        // TODO: not be larger than a single page. For now, default is 4kb
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        for (Map.Entry<String, BsonValue> entry : doc.getFields().entrySet()) {
            String key = entry.getKey();
            BsonValue value = entry.getValue();

            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short) keyBytes.length);
            buffer.put(keyBytes);

            buffer.put(value.bsonType().getCode());

            switch (value.bsonType()) {
                case STRING -> {
                    byte[] valBytes = value.asString().getBytes(StandardCharsets.UTF_8);
                    buffer.putShort((short) valBytes.length);
                    buffer.put(valBytes);
                }
                case INT -> buffer.putInt(value.asInteger());
                case LONG -> buffer.putLong(value.asLong());
                case FLOAT -> buffer.putFloat(value.asFloat());
                case DOUBLE -> buffer.putDouble(value.asDouble());
                case BOOLEAN -> buffer.put((byte) (value.asBoolean() ? 0x01 : 0x00));
            }
        }

        int size = buffer.position();
        byte[] result = new byte[size];
        buffer.flip();
        buffer.get(result);
        return result;
    }
}
