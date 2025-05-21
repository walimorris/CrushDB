package com.crushdb.core.index.btree;

import com.crushdb.core.storageengine.page.Page;

/**
 * Represents a reference to a specific location in a database page.
 *
 * <p>This class holds the page ID and the offset within that page where
 * a particular record or data entry is stored.</p>
 *
 * <h2>Usage Example:</h2>
 * <pre>
 * PageOffsetReference reference = new PageOffsetReference(5L, 128);
 * long pageId = reference.getPageId();  // Retrieves the page ID
 * int offset = reference.getOffset();   // Retrieves the offset
 * </pre>
 *
 * @see Page
 *
 * @author Wali Morris
 * @version 1.0
 */
public class PageOffsetReference {

    /**
     * The unique identifier of the page where data is stored.
     */
    private final long pageId;

    /**
     * The byte offset within the page where the specific record is located.
     */
    private final int offset;

    /**
     * Constructs a {@code PageOffsetReference} with the given page ID and offset.
     *
     * @param p The page ID where the data is stored.
     * @param o The offset within the page.
     */
    public PageOffsetReference(long p, int o) {
        this.pageId = p;
        this.offset = o;
    }

    /**
     * Retrieves the page ID.
     *
     * @return The ID of the page.
     */
    public long getPageId() {
        return this.pageId;
    }

    /**
     * Retrieves the offset within the page.
     *
     * @return The offset in bytes.
     */
    public int getOffset() {
        return this.offset;
    }
}
