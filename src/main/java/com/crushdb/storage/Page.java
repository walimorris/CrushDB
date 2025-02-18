package com.crushdb.storage;

import com.crushdb.model.Document;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;
import java.util.*;

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
    private final long pageId; // unique identifier for each page
    private byte[] page = new byte[4096]; // fixed size of page
    private short availableSpace; // free space in page
    private byte[] compressedPage;
    private int compressedPageSize;
    private long next; // reference to next page
    private long previous; // reference to previous page

    private List<Integer> deletedDocuments; // track deleted documents for reuse
    private Map<Long, Integer> offsets; // document positions in page, quick lookup

    private boolean isFull; // is page full
    private boolean isDirty; // needs to be written to disk? has it been modified
    private boolean isCompressed; // is page compressed?

    private final LZ4Factory lz4factory;

    public Page(long pageId) { // external PageManager will generate ids
        this.pageId = pageId;
        this.availableSpace = 4096 - 128; // accounting for header size
        this.compressedPageSize = -1;
        this.compressedPage = null;
        this.next = -1;
        this.previous = -1;
        this.deletedDocuments = new ArrayList<>();
        this.offsets = new HashMap<>();
        this.isFull = false;
        this.isDirty = false;
        this.isCompressed = false;
        this.lz4factory = LZ4Factory.fastestInstance();
    }

    // TODO: remove
    public Page(byte[] rawPageData) {
        ByteBuffer buffer = ByteBuffer.wrap(rawPageData);

        this.pageId = buffer.getLong();
        this.availableSpace = buffer.getShort();
        this.next = buffer.getLong();
        this.previous = buffer.getLong();
        this.isFull = buffer.get() == 1;
        this.isDirty = buffer.get() == 1;
        this.isCompressed = buffer.get() == 1;
        this.lz4factory = LZ4Factory.fastestInstance();

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
        buffer.get(this.page);
    }

    public void insertDocument(Document document) {
        byte[] data = document.toBytes();
        int totalSize = Integer.BYTES + data.length; // Store document length + content

        if (!hasSpaceFor(totalSize)) {
            throw new IllegalStateException("Not Enough space in this page");
        }
        int insertionPosition;
        if (!this.deletedDocuments.isEmpty()) {
            insertionPosition = this.deletedDocuments.remove(0);
        } else {
            insertionPosition = 4096 - this.availableSpace;
        }
        ByteBuffer buffer = ByteBuffer.wrap(this.page);
        buffer.position(insertionPosition);

        buffer.putInt(data.length);

        buffer.put(data);
        this.offsets.put(document.getDocumentId(), insertionPosition);
        this.availableSpace -= (short) totalSize;

        if (this.availableSpace <= 0) {
            this.isFull = true; // will need to make this check before inserting for split
        }
        this.markDirty();
    }

    public Document retrieveDocument(long documentId) {
        byte[] data = retrieveDocumentBytes(documentId);
        return (data != null) ? Document.fromBytes(data) : null;
    }

    private byte[] retrieveDocumentBytes(long documentId) {
        if (this.isCompressed) {
            decompressPage();
        }
        if (!this.offsets.containsKey(documentId)) {
            return null;
        }
        // use offset to locate the document, get the position in page and return the document bytes.
        // Would it help storing docs with its length prefix?
        int start = this.offsets.get(documentId);
        ByteBuffer buffer = ByteBuffer.wrap(this.page);
        buffer.position(start);

        if (start + 4 > this.page.length) {
            System.err.println("ERROR: Document length prefix exceeds page bounds.");
            return null;
        }

        int length = buffer.getInt();

        if (start + 4 + length > this.page.length) {
            System.err.println("ERROR: Document length exceeds page bounds.");
            return null;
        }
        byte[] document = new byte[length];
        buffer.get(document);

        System.out.println("Retrieved bytes: " + java.util.Arrays.toString(document));

        return document;
    }

    /**
     * Adds document for deletion. Checks if document is in offset.
     * If so, the document is removed from offset and marked for
     * deletion.
     *
     * @param documentId long documentId
     */
    public void deleteDocument(long documentId) {
        if (this.offsets.containsKey(documentId)) {
            int offset = this.offsets.remove(documentId);
            this.deletedDocuments.add(offset);
            this.availableSpace += 128; // TODO: update, get document size
            this.isFull = false;
            markDirty();
        }
    }

    /**
     * Determines if there's space enough in page to add
     * a new document within the available space.
     *
     * @param documentSize int document size
     *
     * @return boolean
     */
    public boolean hasSpaceFor(int documentSize) {
        return this.availableSpace >= documentSize;
    }

    public void serializePage() {
        /*
           1. header:              33
           2. offsets:            500
           3. deleted documents:  500
           4. compress page size:   4
         */
        ByteBuffer buffer = ByteBuffer.allocate(33 + 500 + 500 + 4 + this.compressedPageSize);

        buffer.putLong(this.pageId); // 8 bytes
        buffer.putShort(this.availableSpace); // 2 bytes
        buffer.putLong(this.next); // 8 bytes
        buffer.putLong(this.previous); // 8 bytes
        buffer.put((byte) (this.isFull ? 1 : 0)); // 1 byte
        buffer.put((byte) (this.isCompressed ? 1 : 0)); // 1 byte

        buffer.putInt(this.compressedPageSize); // 4 bytes

        int offsetStart = 33;
        buffer.position(offsetStart);
        for (Map.Entry<Long, Integer> entry : this.offsets.entrySet()) {
            buffer.putLong(entry.getKey());
            buffer.putInt(entry.getValue());
        }

        int deletedDocumentsEnd = offsetStart + 500;
        buffer.position(deletedDocumentsEnd);
        for (Integer deletedOffset : this.deletedDocuments) {
            buffer.putInt(deletedOffset);
        }

        if (this.compressedPage != null) {
            buffer.position(deletedDocumentsEnd + 500);
            buffer.put(this.compressedPage, 0, this.compressedPageSize);
        }
        this.page = buffer.array();
    }

    public void splitPage(byte[] rawPageData) {

    }

    public void compactPage() {

    }

    /**
     * Compress page with LZ4 algorithm.
     *
     * <h2>LZ4-Java Resource:</h2>
     * <p>For more details on how LZ4 compression works, check:</p>
     * <ul>
     *   <li><a href="https://github.com/lz4/lz4-java">Github lz4-java</a></li>
     * </ul>
     */
    public void compressPage() {
        if (!this.isCompressed) {
            LZ4Compressor compressor = this.lz4factory.fastCompressor();

            int dataStart = 33 + 500 + 500 + 4;
            int dataLength = 4096 - dataStart;

            byte[] compressedBuffer = new byte[compressor.maxCompressedLength(dataLength)];
            int compressedSize = compressor.compress(this.page, dataStart, dataLength, compressedBuffer, 0, compressedBuffer.length);

            this.compressedPage = Arrays.copyOf(compressedBuffer, compressedSize);
            this.compressedPageSize = compressedSize;
            this.isCompressed = true;
            this.page = null; // free space after compression
        }
    }

    /**
     * Decompress page with LZ4 algorithm.
     *
     * <h2>LZ4-Java Resource:</h2>
     * <p>For more details on how LZ4 compression works, check:</p>
     * <ul>
     *   <li><a href="https://github.com/lz4/lz4-java">Github lz4-java</a></li>
     * </ul>
     */
    public void decompressPage() {
        if (this.isCompressed) {
            LZ4FastDecompressor decompressor = this.lz4factory.fastDecompressor();
            byte[] restored = new byte[this.page.length];
            int deCompressedLength = decompressor.decompress(this.compressedPage, 0, restored, 0, this.page.length);

            // validate decompression
            if (deCompressedLength != this.page.length) {
                // do something else and clean this up, ex: retry
                throw new RuntimeException("Decompression failure with LZ4 algorithm!");
            }
            this.page = restored;
            this.isCompressed = false;
            this.compressedPage = null;
            this.compressedPageSize = -1;
        }
    }

    public void markDirty() {
        this.isDirty = true;
    }

    public short getAvailableSpace() {
        return this.availableSpace;
    }

    public int getSize() {
        return this.page.length;
    }

    public boolean isCompressed() {
        return this.isCompressed;
    }

    public int getCompressedPageSize() {
        return this.compressedPageSize;
    }
}
