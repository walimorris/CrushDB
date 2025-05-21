package com.crushdb.core.storageengine.journal;

public class JournalEntry {

    public enum OperationType {
        WRITE,
        DELETE;
    }

    private final long timestamp;
    private final OperationType operationType;
    private final String crateName;
    private final long documentId;

    public JournalEntry(long timestamp, OperationType operationType, String crateName, long documentId) {
        this.timestamp = timestamp;
        this.operationType = operationType;
        this.crateName = crateName;
        this.documentId = documentId;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public String getCrateName() {
        return this.crateName;
    }

    public OperationType getOperationType() {
        return this.operationType;
    }

    public long getDocumentId() {
        return this.documentId;
    }
}
