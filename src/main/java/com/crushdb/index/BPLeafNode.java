package com.crushdb.index;

import java.util.Arrays;
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
 *
 * @author Wali Morris
 * @version 1.0
 */
public class BPLeafNode extends BPNode {

    /**
     * Maximum number of key-value pairs in the node (m - 1).
     */
    int maxPairs;

    /**
     * Minimum number of key-value pairs before needing to merge or borrow.
     */
    int minPairs;

    /**
     * Current number of key-value pairs.
     */
    int numPairs;

    /**
     * Left sibling for ordered traversal and borrowing/merging.
     */
    BPLeafNode leftSibling;

    /**
     * Right sibling for ordered traversal and borrowing/merging.
     */
    BPLeafNode rightSibling;

    /**
     * Array of key-value mappings (key → (pageId, offset)).
     */
    BPMapping[] bpMappings;

    /**
     * Creates a new leaf node with an initial key-value pair.
     *
     * @param m The order of the Tree (determines maxPairs).
     * @param mapping The first key-value pair to insert.
     */
    public BPLeafNode(int m, BPMapping mapping) {
        this.maxPairs = m - 1;
        this.minPairs = (int) (Math.ceil(m / 2 ) - 1);
        this.numPairs = 0;
        this.bpMappings = new BPMapping[m];
        this.insert(mapping);
    }

    /**
     * Creates a new leaf node with multiple mappings and a parent.
     * Used when splitting or initializing a Tree with existing keys.
     *
     * @param m The order of the Tree.
     * @param mappings An array of key-value pairs.
     * @param parent The internal node that references this leaf.
     */
    public BPLeafNode(int m, BPMapping[] mappings, BPInternalNode parent) {
        this.maxPairs = m - 1;
        this.minPairs = (int) (Math.ceil(m / 2 )- 1);
        this.bpMappings = mappings;
        this.numPairs = linearSearch(mappings);
        this.parent = parent;
    }

    /**
     * Inserts a key-value mapping into the leaf node.
     *
     * @param mappings The key-value mapping to insert.
     * @return boolean
     */
    public boolean insert(BPMapping mappings) {
        // can't insert if node is full
        if (this.isFull()) {
            return false;
        } else {
            this.bpMappings[numPairs] = mappings;
            numPairs++;
            // maintain order of keys
            Arrays.sort(this.bpMappings, 0, numPairs);
            return true;
        }
    }

    /**
     * Deletes a mapping at a given index.
     * Note: after deletion, merging or borrowing may be required.
     *
     * @param index The index of the key-value pair to delete.
     */
    public void delete(int index) {
        this.bpMappings[index] = null;
        this.numPairs--;
    }

    /**
     * Searches for the first Null slot in the mappings array to count elements.
     *
     * @param mappings The array of key-value mappings.
     * @return The index of the first empty slot, or -1 if full.
     */
    public int linearSearch(BPMapping[] mappings) {
        for (int i = 0; i < mappings.length; i++) {
            if (mappings[i] == null) {
                return i;
            }
        }
        // no empty slots
        return -1;
    }

    /**
     * Checks if the node is full. should/needs to be split.
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
}
