package com.crushdb.index;

import com.crushdb.logger.CrushDBLogger;

import java.util.*;

/**
 * Represents a B+Tree, an ordered tree structure optimized for fast searches, inserts, and deletions.
 * This implementation supports dynamic balancing and range queries.
 *
 * @param <T> The type of keys stored in the tree, which must be Comparable.
 *
 * @author Wali Morris
 * @version 1.0
 */
public class BPTree<T extends Comparable<T>> {
    private static final CrushDBLogger LOGGER = CrushDBLogger.getLogger(BPTree.class);

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
     * Establishing sort type, ASC v. DESC. Default is ASC sort order
     */
    private final SortOrder sortOrder;

    /**
     * Constructs a B+Tree with a given order (m).
     * Initially, the tree starts empty.
     *
     * @param m The order of the tree (maximum number of child nodes per internal node).
     */
    public BPTree(int m) {
        this.m = m;
        this.sortOrder = SortOrder.ASC;
        this.root = null;
    }

    /**
     * Constructs a B+Tree with a given order (m).
     * Initially, the tree starts empty.
     *
     * @param m The order of the tree (maximum number of child nodes per internal node).
     * @param sortOrder Sort order for Tree
     */
    public BPTree(int m, SortOrder sortOrder) {
        this.m = m;
        this.sortOrder = sortOrder;
        this.root = null;
    }

    @SuppressWarnings("unchecked")
    public boolean insert(T key, PageOffsetReference reference) {
        if (this.isEmpty()) {
            this.initialLeafNode = new BPLeafNode<>(this.m, new BPMapping<>(key, reference), sortOrder);
            return true;
        }

        BPLeafNode<T> leafNode = (this.root == null) ? this.initialLeafNode : findLeafNode(key);
        boolean wasInserted = leafNode.insert(new BPMapping<>(key, reference));
        if (wasInserted) {
            return true;
        }
        if (!leafNode.isFull()) {
            return false;
        }

        // force insert for split
        leafNode.forceInsert(new BPMapping<>(key, reference));

        // split the node
        int midpoint = this.mid();
        BPMapping<T>[] rightMappings = splitBPMappings(leafNode, midpoint);

        // new right leaf
        BPInternalNode<T> parent = leafNode.getParent();
        BPLeafNode<T> rightLeaf = new BPLeafNode<>(this.m, rightMappings, parent);
        rightLeaf.setLeftSibling(leafNode);
        rightLeaf.setRightSibling(leafNode.getRightSibling());
        if (rightLeaf.getRightSibling() != null) {
            rightLeaf.getRightSibling().setLeftSibling(rightLeaf);
        }
        leafNode.setRightSibling(rightLeaf);

        // handle parent internal node
        T promoteKey = rightMappings[0].getKey();
        if (parent == null) {
            T[] keys = (T[]) new Comparable[this.m];
            keys[0] = promoteKey;
            BPInternalNode<T> newRoot = new BPInternalNode<>(this.m, keys);
            newRoot.appendChildPointer(leafNode);
            newRoot.appendChildPointer(rightLeaf);
            leafNode.setParent(newRoot);
            rightLeaf.setParent(newRoot);
            this.root = newRoot;
        } else {
            boolean inserted = parent.insertKey(promoteKey);
            if (!inserted) {
                int index = parent.findChildPointerIndex(leafNode);
                parent.forceInsertKey(promoteKey, index);
            }

            int insertIndex = parent.findChildPointerIndex(leafNode) + 1;
            parent.insertChildPointerAtIndex(rightLeaf, insertIndex);
            rightLeaf.setParent(parent);

            // handle internal node splits recursively
            BPInternalNode<T> internal = parent;
            while (internal != null && internal.isOverFull()) {
                splitInternalNode(internal);
                internal = internal.getParent();
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private void splitInternalNode(BPInternalNode<T> internalNode) {
        int midIndex = this.mid();
        T promotedKey = internalNode.getKeys()[midIndex];

        T[] rightKeys = (T[]) new Comparable[this.m];
        BPNode<T>[] rightPointers = (BPNode<T>[]) new BPNode[this.m + 1];

        int rightKeyCount = this.m - midIndex - 1;
        int rightPointerCount = this.m - midIndex;

        System.arraycopy(internalNode.getKeys(), midIndex + 1, rightKeys, 0, rightKeyCount);
        System.arraycopy(internalNode.getChildPointers(), midIndex + 1, rightPointers, 0, rightPointerCount);

        for (int i = midIndex; i < this.m; i++) {
            internalNode.getKeys()[i] = null;
        }
        for (int i = midIndex + 1; i < this.m + 1; i++) {
            internalNode.getChildPointers()[i] = null;
        }

        internalNode.setNumKeys(midIndex);
        internalNode.setChildNodes(midIndex + 1);

        BPInternalNode<T> rightInternalNode = new BPInternalNode<>(this.m, rightKeys, rightPointers, this.sortOrder);
        rightInternalNode.setNumKeys(rightKeyCount);
        rightInternalNode.setChildNodes(rightPointerCount);

        for (int i = 0; i < rightPointerCount; i++) {
            if (rightPointers[i] != null) {
                rightPointers[i].setParent(rightInternalNode);
            }
        }

        BPInternalNode<T> parent = internalNode.getParent();

        if (parent == null) {
            // new root
            T[] rootKeys = (T[]) new Comparable[this.m];
            rootKeys[0] = promotedKey;
            BPInternalNode<T> newRoot = new BPInternalNode<>(this.m, rootKeys);
            newRoot.setNumKeys(1);
            newRoot.setChildNodes(2);

            newRoot.getChildPointers()[0] = internalNode;
            newRoot.getChildPointers()[1] = rightInternalNode;

            internalNode.setParent(newRoot);
            rightInternalNode.setParent(newRoot);
            this.root = newRoot;
        } else {
            int insertIndex = parent.findChildPointerIndex(internalNode);
            boolean inserted = parent.insertKey(promotedKey);
            parent.insertChildPointerAtIndex(rightInternalNode, insertIndex + 1);
            rightInternalNode.setParent(parent);

            if (!inserted && parent.isOverFull()) {
                splitInternalNode(parent);
            }
        }
    }


    @SuppressWarnings("unchecked")
    private BPMapping<T>[] splitBPMappings(BPLeafNode<T> node, int midpoint) {
        BPMapping<T>[] currentMappings = node.getBpMappings();
        int totalPairs = node.getNumPairs();

        BPMapping<T>[] splitMappings = (BPMapping<T>[]) new BPMapping[this.m];

        int j = 0;
        for (int i = midpoint; i < totalPairs; i++) {
            splitMappings[j++] = currentMappings[i];
            currentMappings[i] = null;
        }

        // left node keeps first half
        node.setNumPairs(midpoint);
        return splitMappings;
    }

    /**
     * Searches for a specific key and retrieves its reference.
     *
     * @param key The key to search for.
     *
     * @return The {@link PageOffsetReference} if the key exists, otherwise {@code null}.
     */
    public PageOffsetReference search(T key) {
        if (isEmpty()) {
            return null;
        }
        BPLeafNode<T> leafNode = this.root == null ? this.initialLeafNode : findLeafNode(key);
        BPMapping<T>[] mappings = leafNode.getBpMappings();
        int index = binarySearch(mappings, leafNode.getNumPairs(), key);
        // binary search will return neg int if not found
        if (index < 0) {
            return null;
        } else {
            return mappings[index].reference;
        }
    }

    /**
     * Searches for all keys within a given range and retrieves their references.
     *
     * @param lowerBound The minimum key value (inclusive).
     * @param upperBound The maximum key value (inclusive).
     *
     * @return A list of {@link PageOffsetReference} objects within the range, or {@code null} if empty.
     */
    public ArrayList<PageOffsetReference> rangeSearch(T lowerBound, T upperBound) {
        if (isEmpty()) {
            return null;
        }
        ArrayList<PageOffsetReference> pageOffsetReferences = new ArrayList<>();
        BPLeafNode<T> currentLeafNode = this.initialLeafNode;
        while (currentLeafNode != null) {
            // get mappings and iterate until null
            BPMapping<T>[] mappings = currentLeafNode.getBpMappings();
            for (BPMapping<T> mapping : mappings) {
                if (mapping == null) {
                    break;
                }
                // check that the key is within the bounds, inclusive and add to array
                if (lowerBound.compareTo(mapping.getKey()) <= 0 &&
                        mapping.getKey().compareTo(upperBound) <= 0) {
                    pageOffsetReferences.add(mapping.getReference());
                }
            }
            // go to next leaf - linked
            currentLeafNode = currentLeafNode.getRightSibling();
        }
        return pageOffsetReferences;
    }

    /**
     * Performs a binary search in a sorted array of key-value mappings.
     *
     * @param mappings  The sorted array of {@link BPMapping} objects.
     * @param numPairs  The number of elements in the array.
     * @param key       The key to search for.
     *
     * @return The index of the key if found, otherwise a negative value.
     */
    private int binarySearch(BPMapping<T>[] mappings, int numPairs, T key) {
        Comparator<BPMapping<T>> c = Comparator.comparing(o -> o.key);

        // wrap the key into a BPMapping so it matches the array
        BPMapping<T> searchKey = new BPMapping<>(key, null);
        return Arrays.binarySearch(mappings, 0, numPairs, searchKey, c);
    }

    /**
     * Checks if the tree is empty.
     *
     * @return {@code true} if the tree has no nodes, otherwise {@code false}.
     */
    private boolean isEmpty() {
        return this.initialLeafNode == null;
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
        BPNode<T> node = this.root;
        while (node != null && !(node instanceof BPLeafNode)) {
            BPInternalNode<T> internal = (BPInternalNode<T>) node;
            T[] keys = internal.getKeys();
            int i = 0;
            while (i < internal.getNumKeys() && keys[i] != null && key.compareTo(keys[i]) >= 0) {
                i++;
            }

            // prevent null pointer dereference
            BPNode<T>[] children = internal.getChildPointers();
            if (i >= children.length || children[i] == null) {
                throw new IllegalStateException("Invalid child pointer during traversal (null or out of bounds) at key: " + key);
            }
            node = children[i];
        }
        if (node == null) {
            throw new IllegalStateException("Traversal failed to find a leaf node for key: " + key);
        }
        return (BPLeafNode<T>) node;
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

    public void printLeafNodes() {
        System.out.println("\nLeafNodes");
        System.out.println("________________________");
        BPLeafNode<T> current = this.initialLeafNode;
        while (current != null) {
            BPMapping<T>[] mappings = current.getBpMappings();
            System.out.print("[");
            for (BPMapping<T> mapping: mappings) {
                if (mapping != null) {
                    // theres gap, but works for now and gets visual across
                    System.out.print(mapping.getKey() + " ");
                }
            }
            System.out.print("] ");
            current = current.getRightSibling();
        }
    }
}
