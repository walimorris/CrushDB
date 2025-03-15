package com.crushdb.index;

import com.crushdb.logger.CrushDBLogger;

import static java.lang.String.format;

/**
 * Represents an internal node in a B+ Tree structure.
 * Internal nodes do not store actual data but act as decision points for traversal.
 *
 * <h2>Responsibilities:</h2>
 * <ul>
 *     <li>Stores keys that direct search queries down the tree.</li>
 *     <li>Manages child pointers, which can be other internal nodes or leaf nodes.</li>
 *     <li>Handles insertion, deletion, and search traversal.</li>
 *     <li>Splits when full and merges/borrows when under-filled.</li>
 * </ul>
 *
 * <h2>B+ Tree Integration:</h2>
 * <ul>
 *     <li>Internal nodes serve as decision points for routing queries.</li>
 *     <li>Keys in internal nodes act as separators between child nodes.</li>
 *     <li>They ensure the tree remains balanced by promoting keys when splitting.</li>
 *     <li>When an internal node splits, it promotes a key to its parent node.</li>
 * </ul>
 *
 * <h2>B+ Tree - Internal Node Rules:</h2>
 * <ul>
 *     <li>Stores at most <code>m - 1</code> keys.</li>
 *     <li>Stores at least <code>ceil(m / 2) - 1</code> keys.</li>
 *     <li>Contains exactly <code>m</code> child pointers for <code>m - 1</code> keys.</li>
 *     <li>Keys are stored in sort order {@code ASC(default) or DESC}</li>
 *     <li>All internal nodes (except root) must have at least <code>ceil(m / 2)</code> child nodes.</li>
 *     <li>Root can have a minimum of two child nodes.</li>
 *     <li>When an internal node splits, the middle key is pushed to the parent.</li>
 *     <li>Deletion may trigger merging or borrowing from siblings.</li>
 * </ul>
 *
 * <h2>Sibling Relationships:</h2>
 * <ul>
 *     <li>Internal nodes may have left and right siblings.</li>
 *     <li>Borrowing occurs between siblings when underfilled.</li>
 *     <li>Merging occurs if borrowing is not possible.</li>
 * </ul>
 *
 * TODO: add sort order in constructor
 *
 * @author Wali Morris
 * @version 1.0
 */
public class BPInternalNode<T extends Comparable<T>> extends BPNode<T> {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(BPInternalNode.class);

    /**
     * The maximum number of child nodes the internal node can have.
     */
    private final int maxChildNodes;

    /**
     * The minimum number of child nodes the internal node must have.
     */
    private final int minChildNodes;

    /**
     *  The current number of child nodes the internal node has.
     */
    private int childNodes;

    /**
     * A reference to the previous left internal node at the same level.
     */
    private BPInternalNode<T> leftSibling;

    /**
     * A reference to the previous right internal node at the same level
     */
    private BPInternalNode<T> rightSibling;

    /**
     * Ordered array of keys that separate child nodes.
     */
    private T[] keys;

    /**
     *  Pointers to child nodes. These can be either other internal nodes or leaf nodes.
     */
    private BPNode<T>[] childPointers;

    /**
     * Constructs an internal node in a B+Tree with a given order (m).
     *
     * <p>Initializes an internal node without any child pointers, setting up the maximum
     * and minimum number of children based on the B+Tree order (m). The internal node will
     * have space to store up to {@code maxChildNodes + 1} child pointers to account for an
     * additional pointer during splits.
     *
     * @param m The order of the B+Tree, which determining the maximum number of child nodes.
     * @param keys The keys stored in the internal node. Acts as separators between child nodes.
     */
    @SuppressWarnings("unchecked")
    public BPInternalNode(int m, T[] keys) throws IllegalArgumentException {
        if (keys == null) {
            LOGGER.error(format("Order of tree is %d. Key length must be %d, but key is null.", m, m - 1),
                    IllegalArgumentException.class.getName());
            throw new IllegalArgumentException(format("Order of tree is %d. Key length must be %d, but key is null.",
                    m, m - 1));
        }
        if (keys.length != (m - 1)) {
            LOGGER.error(format("Order of tree is %d. Key length must be %d, but got %d.", m, m - 1, keys.length),
                    IllegalArgumentException.class.getName());
            throw new IllegalArgumentException(format("Order of tree is %d. Key length must be %d, but got %d.",
                    m, m - 1, keys.length));
        }
        this.maxChildNodes = m;
        this.minChildNodes = (int) Math.ceil(m / 2.0);
        this.childNodes = 0;
        this.keys = keys;
        this.childPointers = (BPNode<T>[]) new BPNode[this.maxChildNodes + 1];
    }

    /**
     * Constructs an internal node in a B+Tree with existing keys and child pointers.
     *
     * <p>Used when an internal node is created with keys and child pointers. It initializes
     * the maximum and minimum number of child nodes based on the B+Tree order (m).
     *
     * @param m The order of the B+Tree, which determining the maximum number of child nodes.
     * @param keys The keys stored in the internal node. Acts as separators between child nodes.
     * @param pointers The child node pointers corresponding to the keys.
     */
    public BPInternalNode(int m, T[] keys, BPNode<T>[] pointers) {
        if (keys == null) {
            LOGGER.error(format("Order of tree is %d. Key length must be %d, but key is null.", m, m - 1),
                    IllegalArgumentException.class.getName());
            throw new IllegalArgumentException(format("Order of tree is %d. Key length must be %d, but key is null.",
                    m, m - 1));
        }
        if (keys.length != (m - 1)) {
            LOGGER.error(format("Order of tree is %d. Key length must be %d, but got %d.", m, m - 1, keys.length),
                    IllegalArgumentException.class.getName());
            throw new IllegalArgumentException(format("Order of tree is %d. Key length must be %d, but got %d.",
                    m, m - 1, keys.length));
        }
        this.maxChildNodes = m;
        this.minChildNodes = (int) Math.ceil(m / 2.0);
        // determines number of child nodes
        this.childNodes = linearSearch(pointers);
        this.keys = keys;
        this.childPointers = pointers;
    }

    /**
     * Searches for the first Null slot in the node pointers array.
     *
     * @param nodePointers The array of child pointers in internal nodes.
     * @return The index of the first empty slot, or -1 if full.
     */
    private int linearSearch(BPNode<T>[] nodePointers) {
        for (int i = 0; i < nodePointers.length; i++) {
            if (nodePointers[i] == null) {
                return i;
            }
        }
        // no empty slots
        return -1;
    }

    /**
     * Inserts a child pointer into the internal node.
     *
     * <p>This method adds a reference to a child node at the given index position.
     * Internal nodes do not store actual data but instead store keys and pointers to
     * child nodes. This is the means of searching, these nodes make decisions about
     * which route to go.
     *
     * <h2>Use Cases:</h2>
     * <ul>
     *   <li>When splitting an internal node, new child pointers need to be inserted.</li>
     *   <li>When constructing the tree, inserting the first child pointers.</li>
     * </ul>
     *
     * @param pointer The added child node reference.
     */
    public boolean insertChildPointerAtIndex(BPNode<T> pointer, int index) {
        if (isOverfull()) {
            return false;
        }
        for (int i = this.childNodes - 1; i >= index; i--) {
            this.childPointers[i+1] = childPointers[i];
        }
        this.childPointers[index] = pointer;
        this.childNodes++;
        return true;
    }

    /**
     * Finds the index of a given child pointer in the internal node.
     *
     * <p>Searches through the child pointer array to find the index
     * of a specific child node.
     * <ul>
     *   <li>Determines which subtree to traverse during a search operation.</li>
     *   <li>Finding the child that should be split when inserting new keys.</li>
     *   <li>Locating a child pointer that needs to be adjusted or removed.</li>
     * </ul>
     *
     * <h2>Use Cases:</h2>
     * <ul>
     *   <li>Searching, to determine which subtree to traverse.</li>
     *   <li>Splitting, to locate the child being split.</li>
     *   <li>Deletion, to find and remove the correct child pointer.</li>
     * </ul>
     *
     * @param pointer The child node reference to search.
     * @return The index of the child pointer, or -1.
     */
    public int findChildPointerIndex(BPNode<T> pointer) {
        for (int i = 0; i < this.childNodes; i++) {
            if (this.childPointers[i] == pointer) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Appends a child pointer at the next available position in the childPointers array.
     * This method is typically used when a new child is added during a node split.
     *
     * @param pointer The child node pointer to be added.
     * @return boolean determines if child pointer was inserted or not.
     */
    public boolean appendChildPointer(BPNode<T> pointer) {
        if (isOverfull()) {
            return false;
        }
        this.childPointers[this.childNodes] = pointer;
        this.childNodes++;
        return true;
    }

    /**
     * Prepends a child pointer at the start of the childPointers array.
     * Shifts all existing pointers to the right. Used when redistributing
     * children during a balancing operation.
     *
     * @param pointer The child node pointer to be inserted at the first position.
     * @return boolean determines if child pointer was inserted or not.
     */
    public boolean prependChildPointer(BPNode<T> pointer) {
        if (isOverfull()) {
            return false;
        }
        for (int i = this.childNodes - 1; i >= 0; i--) {
            this.childPointers[i + 1] = this.childPointers[i];
        }
        this.childPointers[0] = pointer;
        this.childNodes++;
        return true;
    }

    /**
     * Removes a key and its associated pointer at the given index.
     * <p>
     * This method ensures that both the key and its corresponding pointer
     * are removed while maintaining the correct tree structure. The child node count
     * is decremented only if both key and pointer removals are valid.
     *
     * TODO: Possibly throw a informational message here in an unlikely turn of events.
     *
     * @param index The index of the key to be removed.
     */
    public void removeKeyAndPointer(int index) {
        boolean validKeyRemoval = removeKeyAtIndex(index, false);
        boolean validPointerRemoval = removePointerAtIndex(index + 1, false);
        if (validKeyRemoval && validPointerRemoval) {
            childNodes--;
        }
    }

    /**
     * Removes a key from the internal node at the given index.
     * <p>
     * This method shifts the remaining keys to maintain order and structure.
     * If the {@code exclusive} flag is set to {@code true}, the child node count
     * is decremented to reflect the change.
     * <p>
     * {@code exclusive} means the key is being removed without also removing
     * any pointer. Consider the relationship between keys and pointers in an
     * internal node.
     *
     * @param index The index of the key to remove.
     * @param exclusive the key is removed exclusively without removal of pointer.
     *
     * @return {@code true} if the removal was successful, otherwise {@code false}.
     */
    public boolean removeKeyAtIndex(int index, boolean exclusive) {
        boolean valid = keyShiftCloseGap(index);
        if (valid && exclusive) {
            this.childNodes--;
        }
        return valid;
    }

    /**
     * Removes a child pointer at the given index.
     * <p>
     * The remaining pointers are shifted to maintain order.
     * If {@code exclusive} is set to {@code true}, the child count is decremented.
     *
     * <p>
     * {@code exclusive} means the pointer is being removed without also removing
     * any key. Consider the relationship between keys and pointers in an
     * internal node.
     *
     * @param index The index of the pointer to remove.
     * @param exclusive the pointer is removed exclusively without removal of key.
     *
     * @return {@code true} if the removal was successful, otherwise {@code false}.
     */
    public boolean removePointerAtIndex(int index, boolean exclusive) {
        boolean valid = pointerShiftCloseGap(index);
        if (valid && exclusive) {
            this.childNodes--;
        }
        return valid;
    }

    /**
     * Removes a specific child pointer from the internal node.
     * <p>
     * This method searches for the given pointer within the child pointers array,
     * shifts the remaining pointers, and decrements the child count if found.
     *
     * @param pointer The pointer to remove.
     *
     * @return The index of the removed pointer, or {@code -1} if not found.
     */
    public int removePointer(BPNode<T> pointer) {
        int index = -1;
        for (int i = 0; i < this.childNodes; i++) {
            if (this.childPointers[i] == pointer) {
                index = i;
                break;
            }
        }
        // index was not found
        if (index == -1) {
            return index;
        }
        pointerShiftCloseGap(index);
        this.childNodes--;
        return index;
    }

    /**
     * Shifts keys to fill a gap left by a removed key.
     * <p>
     * This method ensures that the keys remain in sequential order by shifting
     * elements to the left, maintaining the tree structure.
     * <p>
     * {@code Note: This can leave keys null, not empty or non-existent}
     *
     * @param index The index where the shift should start.
     *
     * @return {@code true} if the shift was successful, otherwise {@code false}.
     */
    private boolean keyShiftCloseGap(int index) {
        // edge case: index less than zero or index out-of-bounds
        // the current number of child nodes. No way - return false
        if (index < 0 || index >= this.keys.length) {
            return false;
        }
        for (int i = index; i < this.keys.length - 1; i++) {
            this.keys[i] = this.keys[i + 1];
        }
        this.keys[this.keys.length - 1] = null;
        return true;
    }

    /**
     * Shifts child pointers to fill a gap left by a removed pointer.
     * <p>
     * This method ensures the child pointer array maintains its correct order
     * by shifting elements leftward.
     *
     * @param index The index where the shift should start.
     *
     * @return {@code true} if the shift was successful, otherwise {@code false}.
     */
    private boolean pointerShiftCloseGap(int index) {
        // edge case: index less than zero or index out-of-bounds
        // the current number of child nodes. No way - return false
        if (index < 0 || index >= this.childNodes) {
            return false;
        }
        for (int i = index; i < this.childNodes - 1; i++) {
            this.childPointers[i] = this.childPointers[i + 1];
        }
        this.childPointers[this.childNodes - 1] = null;
        return true;
    }

    /**
     * Checks if the internal node has fewer children than the required minimum.
     * This is used to determine whether merging or redistribution is needed.
     *
     * @return {@code true} if the node has too few children, otherwise {@code false}.
     */
    public boolean isLacking() {
        return this.childNodes < this.minChildNodes;
    }

    /**
     * Checks if the internal node has more than the minimum required children.
     * This is useful for redistribution, where a sibling can borrow a child.
     *
     * @return {@code true} if the node can share a child, otherwise {@code false}.
     */
    public boolean isShareable() {
        return this.childNodes > this.minChildNodes;
    }

    /**
     * Checks if the internal node is at the exact minimum required children.
     * If merging is needed, this condition must be met.
     *
     * @return {@code true} if the node is at minimum capacity, otherwise {@code false}.
     */
    public boolean isMergeable() {
        return this.childNodes == this.minChildNodes;
    }

    /**
     * Checks if the internal node has exceeded the maximum number of children.
     * If this returns true, the node must be split.
     *
     * @return {@code true} if the node is overfull, otherwise {@code false}.
     */
    public boolean isOverfull() {
        return this.childNodes == maxChildNodes + 1;
    }

    public void setRightSibling(BPInternalNode<T> rightSibling) {
        this.rightSibling = rightSibling;
    }

    public void setLeftSibling(BPInternalNode<T> leftSibling) {
        this.leftSibling = leftSibling;
    }

    public int getMinChildNodes() {
        return minChildNodes;
    }

    public int getMaxChildNodes() {
        return maxChildNodes;
    }

    public int getChildNodes() {
        return childNodes;
    }

    public BPInternalNode<T> getLeftSibling() {
        return leftSibling;
    }

    public BPInternalNode<T> getRightSibling() {
        return rightSibling;
    }

    public T[] getKeys() {
        return keys;
    }

    public BPNode<T>[] getChildPointers() {
        return childPointers;
    }
}
