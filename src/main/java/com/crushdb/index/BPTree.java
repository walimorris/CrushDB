package com.crushdb.index;

/**
 * Represents a B+Tree, an ordered tree structure optimized for fast searches, inserts, and deletions.
 * This implementation supports dynamic balancing and range queries.
 *
 * @param <T> The type of keys stored in the tree, which must be Comparable.
 *
 * TODO: should sort order originate from tree creation?
 * @author Wali Morris
 * @version 1.0
 */
public class BPTree<T extends Comparable<T>> {

    /**
     * The order of the Tree, determining the maximum number of children per internal node.
     * Defines the tree's branching factor. Remains constant.
     */
    private final int m;

    /**
     * The root node of the Tree, which may be an internal node or a leaf node initially.
     * The root changes over time as the tree grows and splits.
     */
    private BPInternalNode<T> root;

    /**
     * The first leaf node in the Tree, used as an entry point for ordered traversal.
     * Helps in efficient range queries.
     */
    private BPLeafNode<T> initialLeafNode;

    /**
     * Constructs a B+Tree with a given order (m).
     * Initially, the tree starts empty.
     *
     * @param m The order of the tree (maximum number of child nodes per internal node).
     */
    public BPTree(int m) {
        this.m = m;
        this.root = null;
    }

    /**
     * Finds the leaf node where a given key belongs.
     * <p>
     * The search starts from the root and traverses down the tree following internal
     * node keys until a leaf node is reached.
     *
     * @param key The key being searched for.
     *
     * @return The leaf node that should contain the given key.
     */
    private BPLeafNode<T> findLeafNode(T key) {
        T[] keys = this.root.getKeys();
        int i;
        for (i = 0; i < this.root.getChildNodes() - 1; i++) {
            if (key.compareTo(keys[i]) < 0) {
                break;
            }
        }
        BPNode<T> childNode = this.root.getChildPointers()[i];
        if (childNode instanceof BPLeafNode<T>) {
            return (BPLeafNode<T>) childNode;
        }
        return findLeafNode((BPInternalNode<T>) childNode, key);
    }

    /**
     * Recursive helper method to locate the correct leaf node for a given key.
     * <p>
     * This method is used internally by {@link #findLeafNode(T)} when the root is an internal node.
     * It follows the keys in internal nodes and determines the next child node to traverse.
     *
     * @param internalNode The internal node currently being traversed.
     * @param key The key being searched for.
     *
     * @return The leaf node where the key should be located or inserted.
     */
    private BPLeafNode<T> findLeafNode(BPInternalNode<T> internalNode, T key) {
        T[] keys = internalNode.getKeys();
        int i;
        for (i = 0; i < internalNode.getChildNodes() - 1; i++) {
            if (key.compareTo(keys[i]) < 0) {
                break;
            }
        }
        BPNode<T> childNode = internalNode.getChildPointers()[i];
        if (childNode instanceof BPLeafNode<T>) {
            return (BPLeafNode<T>) childNode;
        }
        return findLeafNode((BPInternalNode<T>) internalNode.getChildPointers()[i], key);
    }

    /**
     * Finds the index of a given leaf node within an array of child pointers.
     * <p>
     * Used when determining where a specific leaf node is located
     * among a parent's child pointers: deletion, insertion, rebalacning.
     *
     * @param pointers The array of child pointers in an internal node.
     * @param leafNode The specific leaf node which index is being searched for.
     *
     * @return The index of the leaf node in the array, or -1 if not found.
     */
    private int findPointerIndex(BPNode<T>[] pointers, BPLeafNode<T> leafNode) {
        for (int i = 0; i < pointers.length; i++) {
            if (pointers[i] == leafNode) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Calculates the middle index: useful in splitting.
     * <p>
     * When a node becomes overfull, this method can determine the middle index
     * for the split, to help in distributing keys and pointers evenly.
     *
     * @return The middle index based on the tree order (m).
     */
    private int mid() {
        return (int) Math.ceil((this.m + 1) / 2.0 - 1);
    }
}
