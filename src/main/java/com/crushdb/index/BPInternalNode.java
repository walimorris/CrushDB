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
    BPNode<T>[] pointers;

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
        this.pointers = (BPNode<T>[]) new BPNode[this.maxChildNodes + 1];
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
        this.pointers = pointers;
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
