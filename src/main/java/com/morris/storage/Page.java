package com.morris.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a single Page in the CrushDB database storage system.
 *
 * <p>A Page is the fundamental unit of storage in CrushDB.
 * Documents are stored in Pages, and each Page has a fixed size
 * (4KB) to facilitate efficient disk I/O operations.</p>
 *
 * <h2>Page Structure:</h2>
 * A Page consists of:
 * <ul>
 *   <li><b>Header:</b> Metadata such as Page ID, available space, and pointers to other pages.</li>
 *   <li><b>Document Storage:</b> Serialized document data (BSON).</li>
 *   <li><b>Offsets Table:</b> Keeps track of document positions within the page for fast access.</li>
 * </ul>
 *
 * <h2>Why Use Pages?</h2>
 * Pages enable efficient data storage and retrieval by:
 * <ul>
 *   <li>Reducing disk reads/writes by grouping multiple documents into a single block.</li>
 *   <li>Facilitating indexing via B-trees, allowing quick lookups.</li>
 *   <li>Minimizing fragmentation by managing storage space within pages.</li>
 * </ul>
 *
 * <h2>Operations Supported:</h2>
 * The Page class supports:
 * <ul>
 *   <li><b>Insertion:</b> Adding a document if there is enough space.</li>
 *   <li><b>Retrieval:</b> Locating and reading a document using its offset.</li>
 *   <li><b>Deletion:</b> Marking documents as deleted and reclaiming space.</li>
 * </ul>
 *
 * <h2>Additional Resources:</h2>
 * <p>For more details on how pages work in databases, check:</p>
 * <ul>
 *   <li><a href="https://en.wikipedia.org/wiki/Page_(computer_memory)">Page (Computer Memory) - Wikipedia</a></li>
 *   <li><a href="https://www.postgresql.org/docs/current/storage-page-layout.html">PostgreSQL Storage Page Layout</a></li>
 *   <li><a href="https://www.mongodb.com/docs/manual/core/wiredtiger/">MongoDB WiredTiger Storage Engine</a></li>
 * </ul>
 *
 * @author Wali Morris
 * @version 1.0
 */
public class Page {
    private long pageId; // unique identifier for each page
    private final byte[] pageSize = new byte[4096]; // fixed size of page
    private short availableSpace; // free space in page
    private long next; // reference to next page
    private long previous; // reference to previous page

    private List<Integer> deletedDocuments; // track deleted documents for reuse
    private Map<Long, Integer> offsets; // document positions in page, quick lookup

    private boolean isFull; // is page full
    private boolean isDirty; // needs to be written to disk? has it been modified
    private boolean isCompressed; // is page compressed?

    public Page(long pageId) { // external PageManager will generate ids
        this.pageId = pageId;
        this.availableSpace = 4096 - 128; // accounting for header size
        this.next = -1;
        this.previous = -1;
        this.deletedDocuments = new ArrayList<>();
        this.offsets = new HashMap<>();
        this.isFull = false;
        this.isDirty = false;
        this.isCompressed = false;
    }

    public Page(byte[] rawPageData) {
        ByteBuffer buffer = ByteBuffer.wrap(rawPageData);

        this.pageId = buffer.getLong();
        this.availableSpace = buffer.getShort();
        this.next = buffer.getLong();
        this.previous = buffer.getLong();
        this.isFull = buffer.get() == 1;
        this.isDirty = buffer.get() == 1;
        this.isCompressed = buffer.get() == 1;

        // offset table begins at byte 30 after header metadata
        int offsetTableStart = 30;
        int offSetTableEnd = offsetTableStart + 500;

        buffer.position(offsetTableStart);
        while (buffer.position() < offSetTableEnd) {
            long documentId = buffer.getLong();
            int offset = buffer.getInt();
            if (documentId == 0) break;
            this.offsets.put(documentId, offset);
        }

        // start at end of offset table and read next 500 bytes
        int deletedDocumentsEnd = offSetTableEnd + 500;

        buffer.position(offSetTableEnd);
        while (buffer.position() < deletedDocumentsEnd) {
            int deletedOffset = buffer.getInt();
            if (deletedOffset == 0) break;
            this.deletedDocuments.add(deletedOffset);
        }

        // restore document bytes to page, i.e: end of deleted
        // document to remaining page end
        buffer.position(deletedDocumentsEnd);
        buffer.get(this.pageSize);
    }

    public void insertDocument(byte[] data, long documentId) {
        // duplicate id? Prevent overwriting
        if (!hasSpaceFor(data.length)) {
            throw new IllegalStateException("Not Enough space in this page");
            // split
        }
        int insertionPosition;
        if (!this.deletedDocuments.isEmpty()) {
            // removes from deleted docs list and retrieves that element for reuse
            insertionPosition = deletedDocuments.remove(0);
        } else {
            insertionPosition = 4096 - availableSpace;
        }
        System.arraycopy(data, 0, this.pageSize, insertionPosition, data.length);
        this.offsets.put(documentId, insertionPosition);
        this.availableSpace -= (short) data.length;
        markDirty();
    }

    public byte[] retrieveDocument(long documentId) {
        if (!this.offsets.containsKey(documentId)) {
            return null;
        }
        // use offset to locate the document, get the position in page and return the document bytes.
        // Would it help storing docs with its length prefix?
        int start = this.offsets.get(documentId);
        ByteBuffer buffer = ByteBuffer.wrap(this.pageSize);
        buffer.position(start);

        int length = buffer.getInt();
        byte[] document = new byte[length];
        buffer.get(document);

        return document;
    }

    public void deleteDocument(long documentId) {

    }

    public boolean hasSpaceFor(int documentSize) {
        return this.availableSpace >= documentSize;
    }

    public void serializePage(byte[] rawPageData) {

    }

    public void splitPage(byte[] rawPageData) {

    }

    public void compactPage() {

    }

    public void compressPage() {

    }

    public void markDirty() {
        this.isDirty = true;
    }
}
