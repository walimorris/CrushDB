package com.crushdb.storageengine.page;

import com.crushdb.model.Document;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
 *   <li><b>Header (32 bytes):</b> Metadata such as Page ID, available space, and pointers to other pages.</li>
 *   <li><b>Offsets Table:</b> Keeps track of document positions within the page for fast access.</li>
 *   <li><b>Deleted Documents List:</b> Tracks deleted documents for space reclamation and defragmentation.</li>
 *   <li><b>Document Storage:</b> Serialized document data in a structured binary format.</li>
 *   <li><b>Free Space:</b> Unallocated space for future document insertions.</li>
 * </ul>
 *
 * <h2>Page Format:</h2>
 * Each Page is structured as follows:
 * <pre>
 * +------------+----------------+------------------+------------------+------------------+
 * | 32 bytes   | Variable       | Variable         | Variable         | Variable         |
 * | Header     | Offsets Table  | Deleted Docs List| Document Data    | Free Space       |
 * +------------+----------------+------------------+------------------+------------------+
 * </pre>
 * <ul>
 *   <li><b>Header (32 bytes):</b> Contains page metadata such as page ID, available space, total document count, and deletion markers.</li>
 *   <li><b>Offsets Table (variable):</b> A list of pointers to document positions within the page.</li>
 *   <li><b>Deleted Documents List (variable):</b> Tracks documents that have been deleted but still occupy space.</li>
 *   <li><b>Document Data (variable):</b> Stores serialized document bytes.</li>
 *   <li><b>Free Space (variable):</b> Unallocated space for future document insertions.</li>
 * </ul>
 *
 * <h2>Document Format:</h2>
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
 * <h2>Deleted Documents & Space Reclamation:</h2>
 * - Documents are marked as deleted using the **Deleted Flag** instead of being immediately removed.
 * - Deleted documents still occupy space but are skipped during retrieval.
 * - The **Deleted Documents List** keeps track of deleted entries for later reclamation.
 * - A **compaction process** can be triggered to reclaim space by rewriting active documents to a new page.
 *
 *
 * <h2>Why Use Pages?</h2>
 * Pages enable efficient data storage and retrieval by:
 * <ul>
 *   <li>Reducing disk reads/writes by grouping multiple documents into a single block.</li>
 *   <li>Facilitating indexing via B-trees, allowing quick lookups.</li>
 *   <li>Minimizing fragmentation by managing storage space within pages.</li>
 *   <li>Allowing in-place deletions with efficient space reclamation.</li>
 * </ul>
 *
 * <h2>Operations Supported:</h2>
 * The Page class supports:
 * <ul>
 *   <li><b>Insertion:</b> Adding a document if there is enough space.</li>
 *   <li><b>Retrieval:</b> Locating and reading a document using its offset.</li>
 *   <li><b>Deletion:</b> Marking documents as deleted and reclaiming space.</li>
 *   <li><b>Compression:</b> Optionally compressing documents before storage.</li>
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
     * The default page size is 4KB (4096 bytes), containing the header, document metadata,
     * and document storage space.
     */
    private byte[] page;

    /**
     * The actual size (in bytes) of the uncompressed page.
     * This value may be smaller than the default page size if there is unused space.
     */
    private int pageSize;

    /**
     * Header section of the page, containing metadata about the page such as:
     * - Page ID
     * - Available space
     * - Number of documents
     * - Deleted documents list
     * - Checksum for validation
     */
    private byte[] header;

    /**
     * The total size (in bytes) of the page header.
     * The header contains critical metadata about the page.
     */
    private int headerSize;

    /**
     * Tracks available space in the page (in bytes).
     * This value decreases as new documents are added and increases when documents are deleted.
     */
    private short availableSpace;

    private byte[] compressedPage;

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
     * List of document IDS marking  deleted documents within the page.
     */
    private Set<Long> deletedDocuments;

    /**
     * Maps document IDs to their position within the page.
     * Enables fast lookups of document locations without scanning the entire page.
     */
    private Map<Long, Integer> offsets;

    /**
     * indicates whether the page is full (no more space for new documents).
     * If `True`, a new page may need to be created.
     */
    private boolean isFull;

    /**
     * Indicates whether the page has been modified since the last write to storage.
     * Used to determine if the page needs to be flushed to disk or persisted.
     */
    private boolean isDirty;

    /**
     * Indicates whether the page is currently stored in a compressed format.
     * If `true`, `compressedPage` contains the compressed data, and it needs to be
     * decompressed before reading document content.
     */
    private boolean isCompressed;

    /**
     * Used for data integrity validation and stored in header.
     */
    private int checksum;

    /**
     * Timestamp of the last modification to the page.
     * Used for tracking changes and managing page lifecycles.
     */
    private long modificationTimestamp;

    /**
     * Configuration option that determines whether documents should be automatically compressed
     * when inserted into the page.
     * Enabling this feature may improve space efficiency but could increase CPU usage.
     *
     * <p>Default state is `false` (no auto compression).</p>
     */
    private final boolean autoCompressOnInsert;

    private int numberOfDocuments;

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
     * The maximum allowed page size in bytes.
     * Default is 4KB (4096 bytes).
     * <p>
     * This value defines the total memory allocated for storing a page,
     * including its header and document data.
     */
    public static final int MAX_PAGE_SIZE = 0x1000;

    /**
     * The maximum allowed header size in bytes.
     * Default is 128 bytes (0x80).
     * <p>
     * The header stores metadata about the page, including its ID, available space,
     * and pointers to adjacent pages.
     */
    public static final int MAX_HEADER_SIZE = 0x80;

    /**
     * A flag representing an active document.
     * <p>
     * This value (0x01) indicates that a document is currently valid and should be
     * retrievable from the page.
     */
    public static final byte ACTIVE = 0x01;

    /**
     * A flag representing a deleted document.
     * <p>
     * This value (0x00) indicates that a document has been marked as deleted
     * and should no longer be retrievable.
     */
    public static final byte INACTIVE = 0x00;

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public Page(long pageId, boolean autoCompressOnInsert) {
        // TODO: external PageManager will generate ids

        this.pageId = pageId;
        this.page = new byte[MAX_PAGE_SIZE];
        this.headerSize = 32;
        this.header = new byte[this.headerSize];
        this.pageSize = this.headerSize;
        this.availableSpace = (short) (MAX_PAGE_SIZE - this.headerSize);
        this.compressedPageSize = -1;
        this.compressedPage = null;
        this.next = -1;
        this.previous = -1;
        this.deletedDocuments = new HashSet<>();
        this.offsets = new HashMap<>();
        this.isFull = false;
        this.isDirty = false;
        this.isCompressed = autoCompressOnInsert;
        this.checksum = 0;
        this.modificationTimestamp = System.currentTimeMillis();
        this.autoCompressOnInsert = autoCompressOnInsert;
        this.numberOfDocuments = 0;
    }

    public Page(long pageId) {
        this(pageId, false);
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
     *   <li>Apply compression if <code>autoCompressOnInsert</code> is enabled.</li>
     *   <li>Wrap the page in a {@link ByteBuffer} and move to the insertion position.</li>
     *   <li>Store document metadata (ID, decompressed size, compressed size, delete-flag).</li>
     *   <li>Copy the document bytes into the buffer at the insertion position.</li>
     *   <li>Update the <i>offset map</i> to store the document ID and its position.</li>
     *   <li>Adjust the available space and page size.</li>
     *   <li>If the page is now full, mark it as <b>full</b>.</li>
     *   <li>Mark the page as <b>dirty</b> (modified) for persistence tracking.</li>
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
     * @param document The {@link Document} to be inserted.
     * @throws IllegalStateException if the page does not have enough space.
     */
    public Document insertDocument(Document document) {
        byte[] data = document.toBytes();
        int dcs = data.length;
        int cs = 0;
        byte df = ACTIVE;

        if (this.autoCompressOnInsert) {
            data = compressDocument(data);
            cs = data.length;
        }

        int totalSize = DOCUMENT_METADATA_SIZE + (this.autoCompressOnInsert ? cs : dcs);
        int insertionPosition = this.pageSize;

        if (insertionPosition + totalSize > MAX_PAGE_SIZE) {
            throw new IllegalStateException("ERROR: Not enough space in page.");
        }

        // prep buffer to write data to page
        ByteBuffer buffer = ByteBuffer.wrap(this.page);
        buffer.position(insertionPosition);

        // follow the metadata first then write the document content (data)
        buffer.putLong(document.getDocumentId());
        buffer.putLong(this.pageId);
        buffer.putInt(dcs);
        buffer.putInt(cs);
        buffer.put(df);
        buffer.put(data);

        this.offsets.put(document.getDocumentId(), insertionPosition);
        this.availableSpace -= (short) totalSize;
        this.pageSize = insertionPosition + totalSize;

        updateHeader();
        this.markDirty();
        this.numberOfDocuments += 1;
        return retrieveDocument(document.getDocumentId());
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
     * Retrieves the raw byte array of a {@link Document} stored within this page.
     *
     * <p><b>Steps for Retrieval:</b></p>
     * <ol>
     *   <li>Check if the {@code offsets} map contains the {@code documentId}.</li>
     *   <li>Determine the document's starting position using the offset map.</li>
     *   <li>Wrap the page in a {@link ByteBuffer} and move to the document's position.</li>
     *   <li>Read the first 8 bytes (documentId) and validate it matches the expected {@code documentId}.</li>
     *   <li>Read the next 4 bytes (decompressed document size).</li>
     *   <li>Read the next 4 bytes (compressed document size).</li>
     *   <li>Validate the compressed size to ensure it does not exceed the decompressed size.</li>
     *   <li>Read the document content:
     *       <ul>
     *           <li>If the document is compressed, decompress it before returning.</li>
     *           <li>Allocate a new buffer to store the full document metadata and content.</li>
     *           <li>Store the document ID, decompressed size, compressed size, and actual document bytes.</li>
     *       </ul>
     *   </li>
     *   <li>Return the reconstructed document as a byte array.</li>
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
     * <h2>Validation:</h2>
     * <ul>
     *   <li>Ensures that the stored {@code documentId} matches the requested one.</li>
     *   <li>Checks that the document sizes do not exceed page limits.</li>
     *   <li>Validates that decompressed document size matches expectations after decompression.</li>
     * </ul>
     *
     * <p><b>Note:</b> This method assumes that the {@link ByteBuffer} reads data in Big-Endian format,
     * which is Java's default behavior.</p>
     *
     * @param documentId The unique ID of the document to be retrieved.
     * @return A byte array representing the full document (metadata + content).
     *         Returns {@code null} if the document is not found or if an error occurs.
     * @throws IllegalStateException If decompression fails or size mismatches are detected.
     */
    private byte[] retrieveDocumentBytes(long documentId) {
        if (this.deletedDocuments.contains(documentId)) {
            System.err.println("Document with ID " + "'" + documentId + "'" + "marked for deletion.");
            return null;
        }
        if (!this.offsets.containsKey(documentId)) {
            System.err.println("ERROR: Document ID " + documentId + " not found in offsets.");
            return null;
        }
        int start = this.offsets.get(documentId);
        ByteBuffer buffer = ByteBuffer.wrap(this.page);
        buffer.position(start);

        long storedDocId = buffer.getLong();
        long pageId = buffer.getLong();
        int dcs = buffer.getInt();
        int cs = buffer.getInt();
        byte df = buffer.get();

        if (df == INACTIVE) {
            System.out.println("WARNING: Document with ID " + documentId + ", is marked for deletion.");
            // TODO: update document delete flag value
            return null;
        }

        if (storedDocId != documentId) {
            System.err.println("ERROR: Document ID mismatch! Expected: " + documentId + ", Found: " + storedDocId);
            return null;
        }

        // only retrieving documents - so cs remains, no need to update cs value
        int dataSize = (cs > 0) ? cs : dcs;
        byte[] document = new byte[dataSize];
        buffer.get(document);

        if (cs > 0) {
            document = decompressDocument(document, dcs);
            if (document.length != dcs) {
                throw new IllegalStateException("Decompression size mismatch! Expected " + dcs + " but got " + document.length);
            }
        }
        ByteBuffer updatedBuffer = ByteBuffer.allocate(DOCUMENT_METADATA_SIZE + document.length);
        updatedBuffer.putLong(storedDocId);
        updatedBuffer.putLong(pageId);
        updatedBuffer.putInt(dcs);
        updatedBuffer.putInt(cs);
        updatedBuffer.put(df);
        updatedBuffer.put(document);

        return updatedBuffer.array();
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
            // get offset to create tombstone
            // add documentId to deletionMap
            int offset = this.offsets.get(documentId);
            int tombstoneDcs = createTombstone(offset);

            if (tombstoneDcs == -1) {
                System.out.println("Failed to create tombstone for Document with ID: " + documentId);
                this.offsets.put(documentId, offset); // need to put this back for retry
                return;
            } else {
                System.out.println("Tombstone created for document with ID: " + documentId);
            }
            this.deletedDocuments.add(documentId);
            this.numberOfDocuments -= 1;
            markDirty();
        }
    }

    private int createTombstone(int offset) {
        ByteBuffer buffer = ByteBuffer.wrap(this.page);

        buffer.position(offset);
        long docId = buffer.getLong();
        buffer.getLong();
        int dcs = buffer.getInt();
        buffer.getInt();
        byte df = buffer.get();

        if (df == ACTIVE) {
            // move buffer position back to the flag's location and overwrite it
            buffer.position(offset + DOCUMENT_METADATA_SIZE - 1);
            buffer.put(INACTIVE);

            // check flag was correctly updated
            byte tombstoneState = buffer.get(offset + DOCUMENT_METADATA_SIZE - 1);
            if (tombstoneState == INACTIVE) {
                return dcs;
            } else {
                System.err.println("Tombstone with state: " + tombstoneState + " for Document with ID " + docId);
                throw new IllegalStateException("ERROR: Tombstone not flagged for delete for Document with ID: " + docId);
            }
        }
        return -1;
    }

    /**
     * Compresses a document using the LZ4 compression algorithm.
     *
     * <p><b>Steps:</b></p>
     * <ol>
     *   <li>Initialize the LZ4 compression factory.</li>
     *   <li>Determine the maximum compressed length for the given document.</li>
     *   <li>Allocate a buffer large enough to hold the compressed data.</li>
     *   <li>Perform compression and retrieve the actual compressed size.</li>
     *   <li>Return the compressed document trimmed to its actual size.</li>
     * </ol>
     *
     * <h2>Compression Format:</h2>
     * <pre>
     * +-----------------+--------------------+
     * | CompressedSize  | Compressed Content |
     * | (4 bytes)       | (Variable length)  |
     * +-----------------+--------------------+
     * </pre>
     *
     * <p><b>Note:</b> The LZ4 compression is designed for high-speed and low-latency operations.</p>
     *
     * @param rawDocument The uncompressed document bytes.
     * @return A compressed byte array containing the LZ4-compressed data.
     */
    private byte[] compressDocument(byte[] rawDocument) {
        LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = lz4Factory.fastCompressor();

        byte[] compressedBuffer = new byte[compressor.maxCompressedLength(rawDocument.length)];
        int compressedSize = compressor.compress(rawDocument, 0, rawDocument.length, compressedBuffer, 0, compressedBuffer.length);
        return Arrays.copyOf(compressedBuffer, compressedSize);
    }

    /**
     * Decompresses an LZ4-compressed document back to its original form.
     *
     * <p><b>Steps:</b></p>
     * <ol>
     *   <li>Initialize the LZ4 decompression factory.</li>
     *   <li>Allocate a buffer large enough to hold the decompressed content.</li>
     *   <li>Perform decompression from the compressed buffer into the restored buffer.</li>
     *   <li>Return the fully decompressed document.</li>
     * </ol>
     *
     * <h2>Decompression Process:</h2>
     * <pre>
     * +-----------------+--------------------+
     * | CompressedSize  | Compressed Content |
     * | (4 bytes)       | (Variable length)  |
     * +-----------------+--------------------+
     * </pre>
     *
     * <p><b>Note:</b> The decompressed size <b>must</b> be known beforehand to allocate
     * the correct buffer size. This is typically stored as metadata in the page.</p>
     *
     * @param rawCompressedDocument The LZ4-compressed byte array.
     * @param decompressedSize The expected size of the decompressed document.
     * @return A byte array containing the decompressed document.
     * @throws IllegalArgumentException If decompression fails or the sizes mismatch.
     */
    private byte[] decompressDocument(byte[] rawCompressedDocument, int decompressedSize) {
        LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        LZ4FastDecompressor decompressor = lz4Factory.fastDecompressor();

        byte[] restored = new byte[decompressedSize];
        decompressor.decompress(rawCompressedDocument, 0, restored, 0, restored.length);
        return restored;
    }

    /**
     * Determines if there's space enough in page to add
     * a new document within the available space.
     *
     * @param documentSize int document size
     *
     * @return boolean
     */
    private boolean hasSpaceFor(int documentSize) {
        return this.availableSpace >= documentSize;
    }

    public byte[] decompressPage() {
        // one writer allowed
        readWriteLock.writeLock().lock();
        try {
            byte[] decompressedPage = new byte[MAX_PAGE_SIZE];
            ByteBuffer decompressedPageBuffer = ByteBuffer.wrap(decompressedPage);
            Map<Long, Integer> newOffsetMap = new HashMap<>();

            ByteBuffer buffer = ByteBuffer.wrap(this.page);

            // Copy the first 32 bytes of the header
            byte[] header = new byte[this.headerSize];
            buffer.get(header);
            decompressedPageBuffer.put(header);

            for (Map.Entry<Long, Integer> offsetMap : this.offsets.entrySet()) {
                int start = offsetMap.getValue();

                // Move to document position in the page
                buffer.position(start);

                long docId = buffer.getLong();
                long pageId = buffer.getLong();
                int dcs = buffer.getInt();
                int cs = buffer.getInt();
                byte df = buffer.get();

                if (df == INACTIVE) {
                    throw new IllegalStateException("Error: attempting to decompress a page with tombstoned documents.");
                }

                if (docId != offsetMap.getKey()) {
                    throw new IllegalStateException("Error: Document ID mismatch in page decompression.");
                }

                if (cs == 0) {
                    throw new IllegalStateException("Error: attempting to decompress an already decompressed page.");
                }
                byte[] compressedDocumentBytes = new byte[cs];
                buffer.get(compressedDocumentBytes);

                byte[] decompressedDocumentBytes = decompressDocument(compressedDocumentBytes, dcs);
                cs = 0;

                int newOffset = decompressedPageBuffer.position();
                newOffsetMap.put(docId, newOffset);

                decompressedPageBuffer.putLong(docId);
                decompressedPageBuffer.putLong(pageId);
                decompressedPageBuffer.putInt(dcs);
                decompressedPageBuffer.putInt(cs);
                decompressedPageBuffer.put(df);
                decompressedPageBuffer.put(decompressedDocumentBytes);
                this.compressedPageSize = cs;
            }
            this.page = decompressedPage;
            this.offsets = newOffsetMap;
            this.pageSize = decompressedPageBuffer.position();
            this.availableSpace = (short) (MAX_PAGE_SIZE - this.pageSize);
            this.isCompressed = false;
            return decompressedPage;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private Page splitPage() {
        // working on current page - no need to decompress as document headers
        // are read and tombstone docs are removed.
        Map<String, Object> compactState = this.compactPage();
        if (!(boolean) compactState.get("state")) {
            throw new IllegalStateException("Error: split page process fail due to compaction error on Page ID: " + this.pageId);
        }
        // get the compacted page
        Page currentPage = (Page) compactState.get("page");
        int numDocsToSplit = (int) Math.ceil(this.numberOfDocuments * 0.5);
        return null;
    }

    /**
     * Performs in-place page compaction by removing deleted (tombstoned) documents
     * and defragmentation the remaining active documents to reclaim space.
     *
     * <p>During compaction:</p>
     * <ul>
     *   <li>The page header is copied into a new buffer.</li>
     *   <li>Only active documents are copied into the new buffer.</li>
     *   <li>Offsets are updated to reflect new positions of documents.</li>
     *   <li>Available space is recalculated.</li>
     * </ul>
     *
     * <h2>Compaction Process:</h2>
     * <ol>
     *   <li>Create a new page buffer of the same size.</li>
     *   <li>Copy the first {@code headerSize} bytes (header) from the old page.</li>
     *   <li>Iterate over all document offsets:</li>
     *   <ul>
     *       <li>Read document metadata (ID, decompressed size, compressed size, delete flag).</li>
     *       <li>If marked as deleted, skip it and reclaim space.</li>
     *       <li>If active, copy it to the new buffer and update its offset.</li>
     *   </ul>
     *   <li>Replace the old page with the compacted version.</li>
     *   <li>Update metadata: offsets, page size, and available space.</li>
     *   <li>Delete Set storing deleted Document IDs</li>
     * </ol>
     *
     * <h2>Error Handling:</h2>
     * <ul>
     *   <li>If an inconsistency is detected (e.g., offset mismatch), an exception is thrown.</li>
     *   <li>If an error occurs during compaction, an error message is logged.</li>
     * </ul>
     *
     * <h2>Performance Considerations:</h2>
     * <ul>
     *   <li>Compaction is a CPU-intensive operation and should be triggered strategically.</li>
     *   <li>Running compaction too frequently may cause unnecessary performance overhead.</li>
     *   <li>CrushDB wants to crush data to keep a small footprint, so compaction is an
     *       important process. However, we will run benchmarks against various use-cases to
     *       validate the best compaction rates and GraceTimes.
     *    </li>
     * </ul>
     *
     * <p><b>Note:</b>This method does not modify document content, only reorganizes it in memory.</p>
     */
    public Map<String, Object> compactPage() {
        int originalAvailableSpace = this.availableSpace;
        ByteBuffer buffer = ByteBuffer.wrap(this.page);
        ByteBuffer newPageBuffer = ByteBuffer.allocate(MAX_PAGE_SIZE);

        // copy first 32 bytes into new buffer
        byte[] header = new byte[this.headerSize];
        buffer.get(header, 0, this.headerSize);
        newPageBuffer.put(header);

        // like buffer, create new offset map for later swap
        Map<Long, Integer> newOffsetMap = new HashMap<>();

        for (Map.Entry<Long, Integer> offset: this.offsets.entrySet()) {
            long documentId = offset.getKey();
            int offsetValue = offset.getValue();

            // go straight to the offset
            buffer.position(offsetValue);

            // read document header an obtain id, dcs, cs, df
            // there's no need to read the document, it only
            // matters if it is marked for deletion or not.
            long docId = buffer.getLong();
            long pageId = buffer.getLong();
            int dcs = buffer.getInt();
            int cs = buffer.getInt();
            byte df = buffer.get();

            if (docId != documentId) {
                throw new IllegalStateException("Error: offset does not match current document scanned " +
                        "in defragmentation process. Reverting to original page.");
            }

            try {
                // check if document is active - ignoring flagged docs
                // new space is reclaimed at end
                if (df == ACTIVE) {
                    // it's active write to the new buffer, get new offset and add to new offset map
                    int offsetPosition = newPageBuffer.position();
                    newOffsetMap.put(docId, offsetPosition);

                    newPageBuffer.putLong(docId);
                    newPageBuffer.putLong(pageId);
                    newPageBuffer.putInt(dcs);
                    newPageBuffer.putInt(cs);
                    newPageBuffer.put(df);

                    // get document and put in new buffer
                    byte[] document = cs == 0 ? new byte[dcs] : new byte[cs];
                    buffer.get(document);
                    newPageBuffer.put(document);
                }
            } catch (Exception e) {
                System.err.println("ERROR: interruption during defragmentation process: " + e.getLocalizedMessage());
                return Map.of("page", this, "state", false);
            }
        }
        System.out.println("old offset count: " + this.offsets.size());
        System.out.println("new ofsset count: " + newOffsetMap.size());

        this.page = newPageBuffer.array();
        this.pageSize = newPageBuffer.position();
        this.availableSpace = (short) (MAX_PAGE_SIZE - this.pageSize);
        this.offsets = newOffsetMap;

        // It's wild to think there's no way to reclaim these deleted documents - poof gone!
        // this.page reset has officially obliterated any trace off the face of the planet.
        // How many times have I thought how great it'd be to reclaim something that has
        // been accidentally removed, however I guess that defeats the purpose. Bye.. for now.
        this.deletedDocuments.clear();

        System.out.println("Successful Defragmentation on Page with ID " + "'" + this.pageId + "'" + ".");
        System.out.println("Original Available Space: " + originalAvailableSpace + " bytes");
        System.out.println("New Available space: " + this.availableSpace + " bytes");
        System.out.println("Space reclaimed: " + (this.availableSpace - originalAvailableSpace) + " bytes");
        return Map.of("page", this, "state", true);
    }

    /**
     * Updates the page header with metadata about the page state.
     *
     * <p><b>Header Format:</b></p>
     * <pre>
     * +-----------------+------------------+------------+------------+------------+--------------------+-----------------+
     * | Page ID (8B)   | Avail. Space (2B) | Next (8B)  | Prev (8B)  | isFull (1B)| isCompressed (1B)  | Comp. Size (4B) |
     * +-----------------+------------------+------------+------------+------------+--------------------+-----------------+
     * </pre>
     *
     * <p><b>Steps:</b></p>
     * <ol>
     *   <li>Wrap the raw page data in a {@link ByteBuffer} for structured updates.</li>
     *   <li>Write the page metadata fields:
     *       <ul>
     *           <li><b>Page ID (8 bytes):</b> Unique identifier for this page.</li>
     *           <li><b>Available Space (2 bytes):</b> Tracks how much free space is left.</li>
     *           <li><b>Next Page Pointer (8 bytes):</b> Links to the next page in a sequence.</li>
     *           <li><b>Previous Page Pointer (8 bytes):</b> Links to the previous page in a sequence.</li>
     *           <li><b>isFull (1 byte):</b> Indicates whether the page has reached capacity.</li>
     *           <li><b>isCompressed (1 byte):</b> Indicates whether this page is compressed.</li>
     *           <li><b>Compressed Page Size (4 bytes):</b> Stores the compressed size if applicable.</li>
     *       </ul>
     *   </li>
     *   <li>Ensure the header position does not overwrite document data.</li>
     * </ol>
     *
     * <p><b>Future Enhancements:</b></p>
     * <ul>
     *   <li>Add a <b>checksum</b> for integrity verification.</li>
     *   <li>Add a <b>modification timestamp</b> to track last write time.</li>
     * </ul>
     */
    private void updateHeader() {
        // TODO: add checksum and timestamp

        ByteBuffer buffer = ByteBuffer.wrap(this.page);
        buffer.putLong(this.pageId);
        buffer.putShort(this.availableSpace);
        buffer.putLong(this.next);
        buffer.putLong(this.previous);
        buffer.put((byte) (this.isFull ? 1 : 0));
        buffer.put((byte) (this.isCompressed ? 1 : 0));
        buffer.putInt(this.compressedPageSize);

        // Ensure header does not overwrite document data
        buffer.position(this.headerSize);
    }

    /**
     * Marks the page as dirty, indicating that it has been modified
     * since the last write to storage. This flag is used to determine
     * whether the page needs to be persisted to disk.
     */
    private void markDirty() {
        this.isDirty = true;
    }

    public int getPageSize() {
        return this.pageSize;
    }

    public int getCompressedPageSize() {
        return this.compressedPageSize;
    }

    public int getAvailableSpace() {
        return this.availableSpace;
    }

    public int getHeaderSize() {
        return this.headerSize;
    }

    public boolean isAutoCompressOnInsert() {
        return this.autoCompressOnInsert;
    }

    public boolean isCompressed() {
        return this.isCompressed;
    }

    public Set<Long> getDeletedDocuments() {
        return deletedDocuments;
    }

    public int getNumberOfDocuments() {
        return this.numberOfDocuments;
    }
}
