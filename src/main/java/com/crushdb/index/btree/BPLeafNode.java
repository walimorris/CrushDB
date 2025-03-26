package com.crushdb.index.btree;

import java.util.Arrays;
import java.util.Comparator;

import com.crushdb.logger.CrushDBLogger;
import com.crushdb.storageengine.page.Page;

/**
 * Represents a leaf node in a B+ Tree structure.
 * Leaf nodes store the actual key-value mappings.
 *
 * <h2>Responsibilities:</h2>
 * <ul>
 *     <li>Stores key-value pairs (BPMappings) where keys map to page offsets.</li>
 *     <li>Supports ordered traversal through `leftSibling` and `rightSibling` (LinkedList).</li>
 *     <li>Handles insertion, deletion, and search within the node.</li>
 *     <li>Splits when full and merges/borrows when under-filled.</li>
 * </ul>
 *
 * <h2>B+ Tree Integration:</h2>
 * <ul>
 *     <li>Leaf nodes are the end nodes of search queries.</li>
 *     <li>They store the actual document locations in a {@link Page} (PageOffsetReference).</li>
 *     <li>When a leaf node splits, it promotes a key to the internal node above (Parent)</li>
 * </ul>
 * <h2>B+ Tree - Leaf Node rules:</h2>
 * <ul>
 *     <li>Note: The "order(m)" is the maximum number of children an internal node can have.
 *     (see {@link BPInternalNode} for it's rules</li>
 *
 *     <li>Stores at most m - 1 keys</li>
 *     <li>Stores at least ceil(m/2) - 1 keys</li>
 *     <li>Utilizing a doubly linked list for siblings</li>
 *     <li>Leaf nodes do not contain child pointers</li>
 *     <li>All leaf nodes are at the same depth</li>
 *     <li>Contain the actual pointers to data</li>
 *     <li>Keys are stored in sort order {@code ASC(default) or DESC}</li>
 *     <li>Splitting a leaf node creates a new leaf</li>
 *     <li>Deletion may require merging or redistribution</li>
 *     <li>Supports efficient range queries</li>
 * </ul>
 *
 * @author Wali Morris
 * @version 1.0
 */
public class BPLeafNode<T extends Comparable<T>> extends BPNode<T> {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(BPLeafNode.class);

    /**
     * Maximum number of key-value pairs in the node (m - 1).
     */
    private final int maxPairs;

    /**
     * Minimum number of key-value pairs before needing to merge or borrow.
     */
    private final int minPairs;

    /**
     * Current number of key-value pairs.
     */
    private int numPairs;

    /**
     * Left sibling for ordered traversal and borrowing/merging.
     */
    private BPLeafNode<T> leftSibling;

    /**
     * Right sibling for ordered traversal and borrowing/merging.
     */
    private BPLeafNode<T> rightSibling;

    /**
     * Array of key-value mappings (key → (pageId, offset)).
     */
    private BPMapping<T>[] bpMappings;

    /**
     * Establishing sort type, ASC v. DESC. Default is ASC sort order
     */
    private final SortOrder sortOrder;

    /**
     * Comparator for sorting BPMapping keys in descending order.
     */
    private static final Comparator<BPMapping<?>> DESC_COMPARATOR = Comparator.comparing(BPMapping::getKey, Comparator.reverseOrder());

    /**
     * Creates a new leaf node with an initial key-value pair.
     *
     * @param m The order of the Tree (determines maxPairs).
     * @param mapping The first key-value pair to insert.
     */
    @SuppressWarnings("unchecked")
    public BPLeafNode(int m, BPMapping<T> mapping) {
        this.maxPairs = m - 1;
        this.minPairs = (int) (Math.ceil(m / 2.0 ) - 1);
        this.numPairs = 0;
        this.bpMappings = (BPMapping<T>[]) new BPMapping[m];
        this.insert(mapping);
        this.sortOrder = SortOrder.ASC;
    }

    /**
     * Creates a new leaf node with an initial key-value pair.
     *
     * @param m The order of the Tree (determines maxPairs).
     * @param mapping The first key-value pair to insert.
     * @param sortOrder Sort order for leafNode
     */
    @SuppressWarnings("unchecked")
    public BPLeafNode(int m, BPMapping<T> mapping, SortOrder sortOrder) {
        this.maxPairs = m - 1;
        this.minPairs = (int) (Math.ceil(m / 2.0 ) - 1);
        this.numPairs = 0;
        this.bpMappings = (BPMapping<T>[]) new BPMapping[m];
        this.insert(mapping);
        this.sortOrder = sortOrder;
    }

    /**
     * Creates a new leaf node with multiple mappings and a parent.
     * Used when splitting or initializing a Tree with existing keys.
     *
     * @param m The order of the Tree.
     * @param mappings An array of key-value pairs.
     * @param parent The internal node that references this leaf.
     */
    public BPLeafNode(int m, BPMapping<T>[] mappings, BPInternalNode<T> parent) {
        this.maxPairs = m - 1;
        this.minPairs = (int) (Math.ceil(m / 2.0 ) - 1);
        this.bpMappings = mappings;
        this.numPairs = linearSearch(mappings);
        this.parent = parent;
        this.sortOrder = SortOrder.ASC;
    }

    /**
     * Creates a new leaf node with multiple mappings and a parent.
     * Used when splitting or initializing a Tree with existing keys.
     *
     * @param m The order of the Tree.
     * @param mappings An array of key-value pairs.
     * @param sortOrder Sort order for leaf node
     * @param parent The internal node that references this leaf.
     */
    public BPLeafNode(int m, BPMapping<T>[] mappings, BPInternalNode<T> parent, SortOrder sortOrder) {
        this.maxPairs = m - 1;
        this.minPairs = (int) (Math.ceil(m / 2.0 ) - 1);
        this.bpMappings = mappings;
        this.numPairs = linearSearch(mappings);
        this.parent = parent;
        this.sortOrder = sortOrder;
    }

    /**
     * Inserts a key-value mapping into the leaf node.
     *
     * @param mapping The key-value mapping to insert.
     *
     * @return boolean indicating whether the insert was successful or not.
     */
    public boolean insert(BPMapping<T> mapping) throws IllegalArgumentException {
        if (mapping.getKey() == null || mapping.getReferences() == null) {
            LOGGER.error("Keys and References cannot be Null. This will collapse the Tree.",
                    IllegalArgumentException.class.getName()
            );
            throw new IllegalArgumentException("Keys and References cannot be null. This will collapse the Tree.");
        }
        // check if key exist and add reference
        int index = getKey(mapping.getKey());
        if (index >= 0 && this.bpMappings[index].getKey() == mapping.getKey()) {
            BPMapping<T> existingMappings = bpMappings[index];
            existingMappings.getReferences().addAll(mapping.getReferences());
            return true;
        }
        if (this.isFull()) {
            return false;
        }
        this.bpMappings[this.numPairs] = mapping;
        numPairs++;
        if (this.sortOrder == SortOrder.ASC) {
            Arrays.sort(this.bpMappings, 0, numPairs);
        } else {
            Arrays.sort(this.bpMappings, 0, this.numPairs, DESC_COMPARATOR);
        }
        return true;
    }

    /**
     * Forces an insert. Used for indicating when a split needs to occur.
     *
     * @param mapping {@link BPMapping<T>}
     *
     * @return boolean indicates if the force insert was successful
     *
     * @throws IllegalArgumentException numPairs becomes greater than node capacity
     */
    public boolean forceInsert(BPMapping<T> mapping) throws IllegalArgumentException {
        if (mapping == null || mapping.getKey() == null || mapping.getReferences() == null) {
            throw new IllegalArgumentException("Cannot insert null key or reference.");
        }
        if (this.numPairs >= this.bpMappings.length) {
            throw new IllegalArgumentException("Cannot force insert: Node capacity exceeded.");
        }
        this.bpMappings[this.numPairs] = mapping;
        numPairs++;
        if (this.sortOrder == SortOrder.ASC) {
            Arrays.sort(this.bpMappings, 0, this.numPairs);
        } else {
            Arrays.sort(this.bpMappings, 0, this.numPairs, DESC_COMPARATOR);
        }
        return true;
    }

    /**
     * Deletes a mapping at a given index.
     * Note: after deletion, merging or borrowing may be required.
     *
     * @param index The index of the key-value pair to delete.
     */
    public boolean delete(int index) throws IndexOutOfBoundsException {
        if (index < 0 || index >= this.numPairs) {
            // fail fast, but catch in caller (i.e. transaction or storage manager)
            LOGGER.error("Failure to delete key mapping on index: " + index,
                    IndexOutOfBoundsException.class.getName()
            );
            throw new IndexOutOfBoundsException("Failure to delete key mapping on index: " + index);
        }
        // handle shift immediately - leave no gaps
        for (int i = index; i < numPairs - 1; i++) {
            bpMappings[i] = bpMappings[i + 1];
        }
        bpMappings[numPairs - 1] = null;
        numPairs--;
        return true;
    }

    /**
     * Searches for the first Null slot in the mappings array to count elements.
     *
     * @param mappings The array of key-value mappings.
     * @return The index of the first empty slot, or -1 if full.
     */
    public int linearSearch(BPMapping<T>[] mappings) {
        for (int i = 0; i < mappings.length; i++) {
            if (mappings[i] == null) {
                return i;
            }
        }
        // no empty slots
        return -1;
    }

    public int getKey(T key) {
        if (this.sortOrder == SortOrder.ASC) {
            return Arrays.binarySearch(this.bpMappings, 0, this.numPairs, new BPMapping<>(key, null));
        }
        return Arrays.binarySearch(this.bpMappings, 0, this.numPairs, new BPMapping<>(key, null), DESC_COMPARATOR);
    }

    /**
     * Checks if key already exists in {@link BPLeafNode}.
     * {@code NOTE: this only works because of our continuous sorted state}
     *
     * @param key searched key
     *
     * @return boolean
     */
    public boolean containsKey(T key) {
        int index;
        if (this.sortOrder == SortOrder.ASC) {
            index = Arrays.binarySearch(this.bpMappings, 0, this.numPairs, new BPMapping<>(key, null));
            return index >= 0;
        }
        index = Arrays.binarySearch(bpMappings, 0, numPairs, new BPMapping<>(key, null), DESC_COMPARATOR);
        return index >= 0;
    }

    /**
     * Checks if the node is full. should/needs to be split.
     * {@code NOTE: This method should be called, however if it's true
     * then another pair (key) can be inserted because there's space
     * enough for one extra index to handle the split. However, the next
     * decision after the insert should be a split.}
     *
     * @return boolean
     */
    public boolean isFull() {
        return this.numPairs == this.maxPairs;
    }

    /**
     * Checks if the node is under-filled. needs to borrow/merge.
     *
     * @return boolean
     */
    public boolean isLacking() {
        return this.numPairs < this.minPairs;
    }

    /**
     * Checks if the node has extra mappings and can share with a sibling.
     *
     * @return boolean
     */
    public boolean isSharable() {
        return this.numPairs > this.minPairs;
    }

    /**
     * Checks if the node is exactly at minimum capacity. Need for merging.
     *
     * @return boolean
     */
    public boolean isAppendable() {
        return this.numPairs == this.minPairs;
    }

    public void setRightSibling(BPLeafNode<T> rightSibling) {
        this.rightSibling = rightSibling;
    }

    public void setLeftSibling(BPLeafNode<T> leftSibling) {
        this.leftSibling = leftSibling;
    }

    public BPLeafNode<T> getRightSibling() {
        return this.rightSibling;
    }

    public BPLeafNode<T> getLeftSibling() {
        return this.leftSibling;
    }

    public int getMinPairs() {
        return minPairs;
    }

    public int getMaxPairs() {
        return maxPairs;
    }

    public int getNumPairs() {
        return numPairs;
    }

    public void setNumPairs(int numPairs) {
        this.numPairs = numPairs;
    }

    public BPMapping<T>[] getBpMappings() {
        return bpMappings;
    }

    public BPInternalNode<T> getParent() {
        return this.parent;
    }

    public void setParent(BPInternalNode<T> parent) {
        this.parent = parent;
    }
}
