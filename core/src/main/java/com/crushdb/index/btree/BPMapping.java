package com.crushdb.index.btree;

import com.crushdb.storageengine.page.Page;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a key-value mapping used within CrushDB's B+Tree structure.
 *
 * <p>This class maps a key with a {@link PageOffsetReference},
 * which points to a specific location within a {@link Page}.</p>
 *
 * <h2>Usage Example:</h2>
 * <pre>
 * PageOffsetReference reference = new PageOffsetReference(5L, 128);
 * BPMappings mapping = new BPMappings(42L, reference);
 * long key = mapping.getKey();  // Retrieves the mapping key
 * PageOffsetReference ref = mapping.getReference();  // Retrieves the page reference
 *
 * {42L -> {5L, 128}} - DocumentID references the page it's stored and where in the page.
 * </pre>
 *
 * @author Wali Morris
 * @version 1.0
 */
public class BPMapping<T extends Comparable<T>> implements Comparable<BPMapping<T>> {

    /**
     * The key associated with this mapping, used for indexing in the B+Tree.
     * This can be DocumentID, indexed field in document. Initially this will
     * be the DocumentID for naive indexing. Later, we will extend to single
     * field indexes and compound indexes.
     */
    private T key;

    /**
     * A {@link List} of mappings to the page(s) and offset(s) where the data for this
     * key is stored.
     *
     * TODO: would it make sense to also sort the Pages? This may benefit from contiguous seeks
     *
     * @see Page to understand how offsets are stored in CrushDB
     */
    private List<PageOffsetReference> references;

    /**
     * Constructs a {@code BPMappings} object with the specified key and page reference.
     *
     * @param key       The key associated with the reference.
     * @param reference The reference pointing to the data location.
     */
    public BPMapping(T key, PageOffsetReference reference) {
        this.key = key;
        this.references = new ArrayList<>();
        this.references.add(reference);
    }

    @Override
    public int compareTo(BPMapping<T> o) {
        return this.key.compareTo(o.key);
    }

    public T getKey() {
        return key;
    }

    public void addReference(PageOffsetReference reference) {
        this.references.add(reference);
    }

    public List<PageOffsetReference> getReferences() {
        return references;
    }
}
