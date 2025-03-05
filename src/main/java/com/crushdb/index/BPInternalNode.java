package com.crushdb.index;

public class BPInternalNode<T extends Comparable<T>> extends BPNode<T> {

    /**
     * The maximum number of child nodes the internal node can have.
     */
    int maxDegree;

    /**
     * The minimum number of child nodes the internal node must have.
     */
    int minDegree;

    /**
     *  The current number of child nodes the internal node has.
     */
    int degree;

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
}
