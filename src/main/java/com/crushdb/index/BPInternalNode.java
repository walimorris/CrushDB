package com.crushdb.index;

public class BPInternalNode<T extends Comparable<T>> extends BPNode<T> {

    /**
     * The maximum number of child nodes the internal node can have.
     */
    int maxChildNodes;

    /**
     * The minimum number of child nodes the internal node must have.
     */
    int minChildNodes;

    /**
     *  The current number of child nodes the internal node has.
     */
    int childNodes;

    /**
     * A reference to the previous left internal node at the same level.
     */
    BPInternalNode<T> leftSibling;

    /**
     * A reference to the previous right internal node at the same level
     */
    BPInternalNode<T> rightSibling;

    /**
     * Ordered array of keys that separate child nodes.
     */
    T[] keys;

    /**
     *  Pointers to child nodes. These can be either other internal nodes or leaf nodes.
     */
    BPNode<T>[] childPointers;

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
    public BPInternalNode(int m, T[] keys) {
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
        this.maxChildNodes = m;
        this.minChildNodes = (int) Math.ceil(m / 2.0);
        // determines number of child nodes
        this.childNodes = linearSearch(pointers);
        this.keys = keys;
        this.childPointers = pointers;
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
    private void insertChildPointer(BPNode<T> pointer, int index) {
        for (int i = this.childNodes - 1; i >= index; i--) {
            this.childPointers[i+1] = childPointers[i];
        }
        this.childPointers[index] = pointer;
        this.childNodes++;
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
    private int findChildPointerIndex(BPNode<T> pointer) {
        for (int i = 0; i < this.childNodes; i++) {
            if (this.childPointers[i] == pointer) {
                return i;
            }
        }
        return -1;
    }

    private void appendChildPointer(BPNode<T> pointer) {
        this.childPointers[this.childNodes] = pointer;
        this.childNodes++;
    }

    /**
     * Searches for the first Null slot in the node pointers array.
     *
     * @param nodePointers The array of child pointers in internal nodes.
     * @return The index of the first empty slot, or -1 if full.
     */
    public int linearSearch(BPNode<T>[] nodePointers) {
        for (int i = 0; i < nodePointers.length; i++) {
            if (nodePointers[i] == null) {
                return i;
            }
        }
        // no empty slots
        return -1;
    }
}
