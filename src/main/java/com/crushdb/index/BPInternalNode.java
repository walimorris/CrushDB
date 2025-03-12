package com.crushdb.index;

import com.crushdb.logger.CrushDBLogger;

import static java.lang.String.*;

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

    public void removeKeyAndPointer(int index) {
        boolean validKeyRemoval = removeKeyAtIndex(index, false);
        boolean validPointerRemoval = removePointerAtIndex(index + 1, false);
        if (validKeyRemoval && validPointerRemoval) {
            childNodes--;
        }
    }

    public boolean removeKeyAtIndex(int index, boolean exclusive) {
        boolean valid = keyShiftCloseGap(index);
        if (valid && exclusive) {
            this.childNodes--;
        }
        return valid;
    }

    public boolean removePointerAtIndex(int index, boolean exclusive) {
        boolean valid = pointerShiftCloseGap(index);
        if (valid && exclusive) {
            this.childNodes--;
        }
        return valid;
    }

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

    private boolean keyShiftCloseGap(int index) {
        // edge case: index less than zero or index greater than or equal to
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

    private boolean pointerShiftCloseGap(int index) {
        // edge case: index less than zero or index greater than or equal to
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
