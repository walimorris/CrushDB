package com.crushdb.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class Document {
    private final long documentId;
    private final Map<String, String> fields;

    public Document(long documentId) {
        this.documentId = documentId;
        this.fields = new HashMap<>();
    }

    public Map<String, String> getFields() {
        return this.fields;
    }

    public long getDocumentId() {
        return this.documentId;
    }

    public void put(String key, String value) {
        this.fields.put(key, value);
    }

    public String get(String key) {
        return this.fields.get(key);
    }

    public byte[] toBytes() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
        }

        byte[] contentBytes = sb.toString().getBytes(StandardCharsets.UTF_8);

        // store document size before the data
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + contentBytes.length);
        buffer.putLong(documentId);
        buffer.putInt(contentBytes.length);
        buffer.put(contentBytes);

        return buffer.array();
    }

    public static Document fromBytes(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        if (buffer.remaining() < Long.BYTES + Integer.BYTES) { // prevent underflow
            throw new IllegalArgumentException("Invalid byte array: not enough data for documentId.");
        }

        long id = buffer.getLong();
        int contentLength = buffer.getInt();

        if (buffer.remaining() < contentLength) {
            throw new IllegalArgumentException("Invalid byte array: content length mismatch.");
        }

        byte[] contentBytes = new byte[contentLength];
        buffer.get(contentBytes);

        String content = new String(contentBytes, StandardCharsets.UTF_8);
        Document doc = new Document(id);

        String[] pairs = content.split(";");
        for (String pair : pairs) {
            if (!pair.isEmpty()) {
                String[] keyValue = pair.split(":", 2);
                if (keyValue.length == 2) {
                    doc.put(keyValue[0], keyValue[1]);
                }
            }
        }
        return doc;
    }
}
