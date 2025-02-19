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
 *   <li><b>Document Storage:</b> Serialized document data (BSON or raw byte format - TBD).</li>
 *   <li><b>Offsets Table:</b> Keeps track of document positions within the page for fast access.</li>
 * </ul>
 *
 * <h2>Document Format:</h2>
 * Each document stored in a Page follows this structure:
 * <pre>
 * +------------+--------------+------------------+
 * | 8 bytes    | 4 bytes      | Variable         |
 * | documentId | documentSize | documentContent  |
 * +------------+--------------+------------------+
 * </pre>
 * <ul>
 *   <li><b>Document ID (8 bytes):</b> Unique identifier for the document.</li>
 *   <li><b>Document Size (4 bytes):</b> Length of the document content.</li>
 *   <li><b>Document Content (n bytes):</b> The actual serialized document data.</li>
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

    /**
     * Unique identifier for this page.
     * Used for referencing the page within storage.
     */
    private final long pageId;

    /**
     * Fixed-size byte array representing the raw data of this page.
     * The default page size is 4KB (4096 bytes)
     */
    private byte[] page;

    /**
     * The size (bytes) of the uncompressed page.
     */
    private int pageSize;

    /**
     * Tracks available space in the page (bytes).
     * This value decreases as new documents are added.
     */
    private short availableSpace;

    /**
     * Stores the compressed version of this page (if compression is enabled).
     * Used to save storage space.
     */
    private byte[] compressedPage;

    /**
     * The size (bytes) of the compressed page.
     * This is different from `page.length` since compression reduces size.
     */
    private int compressedPageSize;

    /**
     * Reference to the next page in a sequence (if applicable).
     * Used for linked-page traversal.
     */
    private long next;

    /**
     * Reference to the previous page in a sequence (if applicable).
     * Enables bidirectional traversal.
     */
    private long previous;

    /**
     * List of offsets marking where deleted documents were stored.
     * These positions can be reused for new inserts at Earliest Deleted Document (EDD).
     */
    private List<Integer> deletedDocuments;

    /**
     * Maps document IDs to their position within the page.
     * Enables quick lookup of document locations.
     */
    private Map<Long, Integer> offsets;

    /**
     * indicates whether the page is full (no more space for new documents).
     * If `True`, a new page may need to be created.
     */
    private boolean isFull;

    /**
     * Indicates whether the page has been modified since the last write to storage.
     * Used to determine if the page needs to be persisted.
     */
    private boolean isDirty;

    /**
     * Indicates whether the page is currently stored in a compressed format.
     * If `True`, `compressedPage` contains the compressed data.
     */
    private boolean isCompressed;

    public Page(long pageId) { // external PageManager will generate ids
        this.pageId = pageId;
        this.page = new byte[4096];
        this.pageSize = 0;
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

    /**
     * Inserts a {@link Document} into the page.
     *
     * <p><b>Steps for Insertion:</b></p>
     * <ol>
     *   <li>Convert the document into a byte array representation.</li>
     *   <li>Check if there is enough available space in the page.</li>
     *   <li>Determine the insertion position:
     *       <ul>
     *           <li>If space is available, use the next free position.</li>
     *           <li>If an <i>earliest deleted document</i> (EDD) exists, reuse its position.</li>
     *       </ul>
     *   </li>
     *   <li>Wrap the page in a {@link ByteBuffer} and move to the insertion position.</li>
     *   <li>Copy the document bytes into the buffer at the insertion position.</li>
     *   <li>Update the <i>offset map</i> to store the document ID and its position.</li>
     *   <li>If the page is now full, mark it as <b>full</b>.</li>
     *   <li>Mark the page as <b>dirty</b> (modified) for persistence tracking.</li>
     * </ol>
     *
     * @param document The {@link Document} to be inserted.
     * @throws IllegalStateException if the page does not have enough space.
     */
    public void insertDocument(Document document) {
        if (this.isCompressed()) {
            this.decompressPage();
        }
        byte[] data = document.toBytes();
        int totalSize = Long.BYTES + Integer.BYTES + data.length;

        if (!hasSpaceFor(data.length)) {
            throw new IllegalStateException("Not Enough space in this page");
        }
        int insertionPosition;
        if (!this.deletedDocuments.isEmpty()) {
            insertionPosition = this.deletedDocuments.remove(0);
        } else {
            insertionPosition = this.page.length - this.availableSpace;
        }
        ByteBuffer buffer = ByteBuffer.wrap(this.page);
        buffer.position(insertionPosition);

        buffer.put(data);
        this.offsets.put(document.getDocumentId(), insertionPosition);
        this.availableSpace -= (short) totalSize;

        if (this.availableSpace <= 0) {
            // will need to make this check before inserting for split
            this.isFull = true;
        }
        this.pageSize = this.page.length - availableSpace;
        this.compressPage();
        this.markDirty();
    }

    /**
     * Retrieves a {@link Document}.
     *
     * @param documentId long - the documentId to be retrieved.
     *
     * @return {@link Document}
     */
    public Document retrieveDocument(long documentId) {
        byte[] data = retrieveDocumentBytes(documentId);
        return (data != null) ? Document.fromBytes(data) : null;
    }

    /**
     * Retrieves a {@link Document} bytes[].
     *
     * <p><b>Steps for Retrieval:</b></p>
     * <ol>
     *   <li>If document is compressed, decompress the document before reading.</li>
     *   <li>Check if offsets map contains the documentId.</li>
     *   <li>Determine the document position in offset map.</li>
     *   <li>Wrap the page in a {@link ByteBuffer} and move to document's position.</li>
     *   <li>Read the first 8 bytes (documentId) and validate it matches the expected documentId.</li>
     *   <li>Read the next 4 bytes (document size) and calculate total size.</li>
     *   <li>Check and validate that the document length does not exceed the page bounds.</li>
     *   <li>Allocate a byte array with enough space for the full document:</li>
     *   <ul>
     *      <li><b>Metadata (12 bytes):</b> documentId (8 bytes) + document size (4 bytes).</li>
     *      <li><b>Content (n bytes - variable length):</b> Actual serialized document.</li>
     *   </ul>
     *   <li>Move Buffer pointer tto the document's starting position (based on offset)</li>
     *   <li>Read the entire document (documentId, document size, content) into document byte[] array</li>
     * </ol>
     *
     * <h2>Byte Storage Format:</h2>
     * <pre>
     * +------------+--------------+------------------+
     * | 8 bytes    | 4 bytes      | Variable         |
     * | documentId | documentSize | documentContent  |
     * +------------+--------------+------------------+
     * </pre>
     *
     * <p><b>Note:</b> This method assumes that the {@link ByteBuffer} reads data in Big-Endian format,
     * which is Java's default behavior.</p>
     *
     * @param documentId the documentId to be retrieved.
     * @return byte[] - the raw byte data of the document, including metadata.
     */
    private byte[] retrieveDocumentBytes(long documentId) {
        if (this.isCompressed()) {
            decompressPage();
        }
        if (!this.offsets.containsKey(documentId)) {
            return null;
        }

        int start = this.offsets.get(documentId);
        ByteBuffer buffer = ByteBuffer.wrap(this.page);

        buffer.position(start);
        long storedDocId = buffer.getLong();

        if (storedDocId != documentId) {
            System.err.println("ERROR: Document ID mismatch! Expected: " + documentId + ", Found: " + storedDocId);
            return null;
        }

        // 4 bytes of documentSize
        int documentSize = buffer.getInt();

        // includes documentId(8b), documentSize(4b), documentContent(variable nB)
        int totalLength = Long.BYTES + Integer.BYTES + documentSize;
        if (start + totalLength > this.page.length) {
            System.err.println("ERROR: Document length exceeds page bounds.");
            return null;
        }
        byte[] document = new byte[totalLength];
        buffer.position(start);
        buffer.get(document);
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
            this.availableSpace += (short) offset;

            if (this.availableSpace > 0) {
                this.isFull = false;
            }
            this.pageSize = this.page.length - this.availableSpace;
            this.compressPage();
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
        if (!this.isCompressed()) {
            LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
            LZ4Compressor compressor = lz4Factory.fastCompressor();

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
        if (this.isCompressed()) {
            LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
            LZ4FastDecompressor decompressor = lz4Factory.fastDecompressor();
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
