package com.crushdb.index;

import com.crushdb.index.btree.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class BPInternalNodeTest {

    @Test
    void insertChildPointerAtIndex() {
        // child nodes are equal to order of tree(m), however the data structure can hold
        // m + 1 child nodes to account for splits
        Long[] keys = {12L, 13L, 14L};

        // placeholder keys
        Long[] placeHolderKeys = {1L, 2L, 3L};

        // we will point to other internal nodes
        BPInternalNode<Long> childNode1 = new BPInternalNode<>(4, placeHolderKeys);
        BPInternalNode<Long> childNode2 = new BPInternalNode<>(4, placeHolderKeys);
        BPInternalNode<Long> childNode3 = new BPInternalNode<>(4, placeHolderKeys);
        BPInternalNode<Long> childNode4 = new BPInternalNode<>(4, placeHolderKeys);
        BPInternalNode<Long> childNode5 = new BPInternalNode<>(4, placeHolderKeys);

        BPInternalNode<Long> internalNode = new BPInternalNode<>(4, keys);

        assertEquals(0, internalNode.getChildNodes());

        boolean insert1 = internalNode.insertChildPointerAtIndex(childNode1, 0);
        boolean insert2 = internalNode.insertChildPointerAtIndex(childNode2, 1);
        boolean insert3 = internalNode.insertChildPointerAtIndex(childNode3, 1);

        assertAll(
                () -> assertEquals(3, internalNode.getChildNodes()),
                () -> assertTrue(insert1),
                () -> assertTrue(insert2),
                () -> assertTrue(insert3)
        );

        // validate order of nodes
        // node1 = 0, node2 = 2, node3 = 1 (this pushed node2 to the right at index 2 to insert node3 at index 1)
        assertAll(
                () -> assertEquals(childNode1, internalNode.getChildPointers()[0]),
                () -> assertEquals(childNode3, internalNode.getChildPointers()[1]),
                () -> assertEquals(childNode2, internalNode.getChildPointers()[2])
        );

        boolean insert4 = internalNode.insertChildPointerAtIndex(childNode4, internalNode.getChildNodes());

        // this calls insert on the max child value (5) - and will deny insert because pointers is full
        boolean insert5 = internalNode.insertChildPointerAtIndex(childNode5, internalNode.getChildNodes());

        assertAll(
                () -> assertTrue(insert4),
                () -> assertTrue(insert5),
                () -> assertFalse(internalNode.isPointersFull()),

                // account for zero index with max four nodes
                () -> assertEquals(childNode4, internalNode.getChildPointers()[3]),
                () -> assertEquals(childNode5, internalNode.getChildPointers()[internalNode.getMaxChildNodes()])
        );
    }

    @Test
    void findChildPointerIndex() {
        String[] keys = {"United States", "Peru"};

        // placeholder keys
        String[] placeHolderKeys = {"Apple", "Orange"};

        BPInternalNode<String> pointer1 = new BPInternalNode<>(3, placeHolderKeys);
        BPInternalNode<String> pointer2 = new BPInternalNode<>(3, placeHolderKeys);
        BPInternalNode<String> pointer3 = new BPInternalNode<>(3, placeHolderKeys);

        BPInternalNode<String> internalNode = new BPInternalNode<>(3, keys);
        boolean insert1 = internalNode.insertChildPointerAtIndex(pointer1, 0);
        boolean insert2 = internalNode.insertChildPointerAtIndex(pointer2, 0);
        int find1 = internalNode.findChildPointerIndex(pointer1);
        int find2 = internalNode.findChildPointerIndex(pointer2);
        int find3 = internalNode.findChildPointerIndex(pointer3);

        assertAll(
                () -> assertTrue(insert1),
                () -> assertTrue(insert2),
                () -> assertEquals(1, find1),
                () -> assertEquals(0, find2),
                () -> assertEquals(-1, find3)
        );
    }

    @Test
    void appendChildPointer() {
        // placeholder keys
        String[] placeHolderKeys = {"Apple", "Orange"};

        BPInternalNode<String> pointer1 = new BPInternalNode<>(3, placeHolderKeys);
        BPInternalNode<String> pointer2 = new BPInternalNode<>(3, placeHolderKeys);
        BPInternalNode<String> pointer3 = new BPInternalNode<>(3, placeHolderKeys);
        BPInternalNode<String> pointer4 = new BPInternalNode<>(3, placeHolderKeys);

        BPInternalNode<String> parent = new BPInternalNode<>(3, placeHolderKeys);
        boolean insert1 = parent.insertChildPointerAtIndex(pointer4, 0);
        boolean insert2 = parent.appendChildPointer(pointer2);
        boolean insert3 = parent.appendChildPointer(pointer1);

        // max is 3 child-child pointers, after which the node is full - a 4th insert needs split
        boolean insert4 = parent.appendChildPointer(pointer3);

        // append insert order -> pointer4, pointer2, pointer1
        assertAll(
                () -> assertTrue(insert1),
                () -> assertTrue(insert2),
                () -> assertTrue(insert3),
                () -> assertFalse(insert4),
                () -> assertEquals(pointer4, parent.getChildPointers()[0]),
                () -> assertEquals(pointer2, parent.getChildPointers()[1]),
                () -> assertEquals(pointer1, parent.getChildPointers()[2]),
                // max = 3
                () -> assertNull(parent.getChildPointers()[parent.getMaxChildNodes()])
        );
    }

    @Test
    void prependChildPointer() {
        // placeholder keys
        Long[] placeHolderKeys = {1L};

        // let's point to a leaf node - bp internal node and bp leaf node extends bp node
        BPLeafNode<Long> pointer1 = new BPLeafNode<>(3, new BPMapping<>(1L, new PageOffsetReference(10L, 36)));
        BPLeafNode<Long> pointer2 = new BPLeafNode<>(3, new BPMapping<>(2L, new PageOffsetReference(20L, 72)));
        BPLeafNode<Long> pointer3 = new BPLeafNode<>(3, new BPMapping<>(3L, new PageOffsetReference(30L, 144)));

        // max = 2, pointersMax = 3,  max pointer index will be index[1]
        BPInternalNode<Long> parent = new BPInternalNode<>(2, placeHolderKeys);
        boolean insert1 = parent.insertChildPointerAtIndex(pointer1, 0);
        boolean insert2 = parent.prependChildPointer(pointer3);
        boolean insert3 = parent.prependChildPointer(pointer2);

        // we've attempted to insert 3 nodes, this is full
        assertAll(
                () -> assertTrue(parent.isPointersFull()),
                () -> assertTrue(insert1),
                () -> assertTrue(insert2),
                () -> assertFalse(insert3)
        );

        // the max child nodes should be 2(m) - however to account for splitting we leave space
        // for one more node pointer (3), so the current number of child nodes should be the max of 2
        assertEquals(2, parent.getChildNodes());
        assertEquals(2, parent.getMaxChildNodes());
        assertEquals(3, parent.getChildPointers().length);

        // we can count on the latest inserts being the earliest in the pointers array
        assertAll(
                () -> assertEquals(pointer3, parent.getChildPointers()[0]),
                () -> assertEquals(pointer1, parent.getChildPointers()[1]),
                // we can count on the last pointer being the limit (account for zero index)
                // and it being null since max was reached and insert failed. In this case
                // a forced insert is done. see forceInsert test
                () -> assertNull(parent.getChildPointers()[parent.getMaxChildNodes()])
        );

        // let's visualize this
        String result = "[3, 1, NULL]";

        assertEquals(result, visualizeChildNodePointers(parent));

        // add force insert here
        String forceInsertResult = "[3, 1, 2]";
        boolean forceInsert = parent.forceInsertChildPointer(pointer2, parent.getMaxChildNodes());
        assertTrue(forceInsert);
        assertNotNull(parent.getChildPointers()[parent.getMaxChildNodes()]);
        assertEquals(forceInsertResult, visualizeChildNodePointers(parent));
    }

    private String visualizeChildNodePointers(BPInternalNode<?> internalNode) {
        StringBuilder resultString = new StringBuilder();
        resultString.append("[");
        for (int i = 0; i < internalNode.getChildPointers().length; i++) {
            BPLeafNode<?> leafNode = (BPLeafNode<?>) internalNode.getChildPointers()[i];
            if (leafNode == null && !(i == internalNode.getMaxChildNodes())) {
                resultString.append("NULL").append(",");
            } else if (leafNode == null && (i == internalNode.getMaxChildNodes())) {
                resultString.append("NULL");
            } else if (leafNode != null && (i == internalNode.getMaxChildNodes())) {
                resultString.append(leafNode.getBpMappings()[0].getKey());
            } else {
                if (leafNode != null) {
                    resultString.append(leafNode.getBpMappings()[0].getKey()).append(", ");
                }
            }
        }
        resultString.append("]");
        return resultString.toString();
    }

    @Test
    void removeKeyAndPointer() {
        // create a node with 4 nodes and 3 keys. The structure should look like such
        // P0 | K1 | P1 | K2 | P2 | K3 | P3
        // we can see that keys direct to pointers, in this case deleting K1 key, deletes the P1 Pointer
        //and hence keeps the internal node balanced in this way, until the tree solves for splits and merges
        // P0 is special, it is a pointer to values < K1. Note that tree class will prepend or append based
        // on value so we can just add these indexes in sorted order now
        String[] keys = {"Columbia", "Kenya", "United States"};
        BPLeafNode<String> pointer0 = new BPLeafNode<>(4, new BPMapping<>("Nigeria", new PageOffsetReference(1L, 36)));
        BPLeafNode<String> pointer1 = new BPLeafNode<>(4, new BPMapping<>("United Kingdom", new PageOffsetReference(2L, 72)));
        BPLeafNode<String> pointer2 = new BPLeafNode<>(4, new BPMapping<>("Liberia", new PageOffsetReference(3L, 144)));
        BPLeafNode<String> pointer3 = new BPLeafNode<>(4, new BPMapping<>("Brazil", new PageOffsetReference(4L, 36)));
        BPInternalNode<String> internalNode = new BPInternalNode<>(4, keys);

        // we can imagine tree logic would insert pointers correctly
        internalNode.appendChildPointer(pointer3); // "Brazil" (P0)
        internalNode.appendChildPointer(pointer2); // "Liberia" (P1)
        internalNode.appendChildPointer(pointer0); // "Nigeria" (P2)
        internalNode.appendChildPointer(pointer1); // "UK" (P3)

        String initialKeysArray = "[Columbia, Kenya, United States]";
        String initialPointerArray = "[Brazil, Liberia, Nigeria, United Kingdom]";

        assertEquals(initialKeysArray, Arrays.toString(internalNode.getKeys()));
        assertEquals(initialPointerArray, createChildPointersArray(internalNode));
        assertEquals(4, internalNode.getChildNodes());

        // removing K1 also remove P1 - this means Columbia (key) and Nigeria (pointer) are deleted
        String deletionKeyArray1 = "[Kenya, United States, null]";
        String deletionPointerArray1 = "[Brazil, Nigeria, United Kingdom]";
        internalNode.removeKeyAndPointer(0);

        assertEquals(deletionKeyArray1, Arrays.toString(internalNode.getKeys()));
        assertEquals(deletionPointerArray1, createChildPointersArray(internalNode));
        assertEquals(3, internalNode.getChildNodes());

        // removing K2 also remove P2 - this means US (key) and UK (pointer) are deleted
        String deletionKeyArray2 = "[Kenya, null, null]";
        String deletionPointerArray2 = "[Brazil, Nigeria]";
        internalNode.removeKeyAndPointer(1);

        assertEquals(deletionKeyArray2, Arrays.toString(internalNode.getKeys()));
        assertEquals(deletionPointerArray2, createChildPointersArray(internalNode));
        assertEquals(2, internalNode.getChildNodes());
    }

    @SuppressWarnings("unchecked")
    private String createChildPointersArray(BPInternalNode<?> internalNode) {
        StringBuilder pointersArray = new StringBuilder("[");
        BPNode<?>[] childPointers = internalNode.getChildPointers();
        int validChildNodes = internalNode.getChildNodes();
        for (int i = 0; i < validChildNodes; i++) {
            BPLeafNode<String> current = (BPLeafNode<String>) childPointers[i];
            if (current != null) {
                pointersArray.append(current.getBpMappings()[0].getKey());
                if (i < validChildNodes - 1) {
                    pointersArray.append(", ");
                }
            }
        }
        pointersArray.append("]");
        return pointersArray.toString();
    }

    @Test
    void removeKeyAtIndex() {
        Long[] keys = {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L};
        BPInternalNode<Long> internalNode = new BPInternalNode<>(12, keys);

        String initialKeys = "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]";
        assertEquals(initialKeys, Arrays.toString(internalNode.getKeys()));

        boolean removal1 = internalNode.removeKeyAtIndex(5, true);
        String removalArray1 = "[0, 1, 2, 3, 4, 6, 7, 8, 9, 10, null]";
        assertTrue(removal1);
        assertEquals(removalArray1, Arrays.toString(internalNode.getKeys()));

        boolean removal2 = internalNode.removeKeyAtIndex(8, false);
        String removalArray2 = "[0, 1, 2, 3, 4, 6, 7, 8, 10, null, null]";
        assertTrue((removal2));
        assertEquals(removalArray2, Arrays.toString(internalNode.getKeys()));

        // test exclusive removals with child nodes!
    }

    @Test
    void removePointerAtIndex() {
        String[] placeHolderKeys = {"Apple", "Orange", "Banana"};
        BPLeafNode<String> pointer0 = new BPLeafNode<>(4, new BPMapping<>("Nigeria", new PageOffsetReference(1L, 36)));
        BPLeafNode<String> pointer1 = new BPLeafNode<>(4, new BPMapping<>("United Kingdom", new PageOffsetReference(2L, 72)));
        BPLeafNode<String> pointer2 = new BPLeafNode<>(4, new BPMapping<>("Liberia", new PageOffsetReference(3L, 144)));
        BPLeafNode<String> pointer3 = new BPLeafNode<>(4, new BPMapping<>("Brazil", new PageOffsetReference(4L, 36)));
        BPInternalNode<String> internalNode = new BPInternalNode<>(4, placeHolderKeys);

        // we can imagine tree logic would insert pointers correctly
        internalNode.appendChildPointer(pointer3); // "Brazil" (P0)
        internalNode.appendChildPointer(pointer2); // "Liberia" (P1)
        internalNode.appendChildPointer(pointer0); // "Nigeria" (P2)
        internalNode.appendChildPointer(pointer1); // "UK" (P3)

        String initialPointerArray = "[Brazil, Liberia, Nigeria, United Kingdom]";
        assertEquals(4, internalNode.getChildNodes());
        assertEquals(initialPointerArray, createChildPointersArray(internalNode));

        // remove P2 - Liberia
        String pointerArray1 = "[Brazil, Liberia, United Kingdom]";
        boolean removal1 = internalNode.removePointerAtIndex(2, true);
        assertTrue(removal1);
        assertEquals(3, internalNode.getChildNodes());
        assertEquals(pointerArray1, createChildPointersArray(internalNode));

        // remove new P0
        String pointerArray2 = "[Liberia, United Kingdom]";
        boolean removal2 = internalNode.removePointerAtIndex(0, true);
        assertTrue(removal2);
        assertEquals(2, internalNode.getChildNodes());
        assertEquals(pointerArray2, createChildPointersArray(internalNode));
    }

    @Test
    void removePointer() {
        String[] placeHolderKeys = {"Apple", "Orange", "Banana"};
        BPLeafNode<String> pointer0 = new BPLeafNode<>(4, new BPMapping<>("Nigeria", new PageOffsetReference(1L, 36)));
        BPLeafNode<String> pointer1 = new BPLeafNode<>(4, new BPMapping<>("United Kingdom", new PageOffsetReference(2L, 72)));
        BPLeafNode<String> pointer2 = new BPLeafNode<>(4, new BPMapping<>("Liberia", new PageOffsetReference(3L, 144)));
        BPLeafNode<String> pointer3 = new BPLeafNode<>(4, new BPMapping<>("Brazil", new PageOffsetReference(4L, 36)));

        BPLeafNode<String> pointer4 = new BPLeafNode<>(4, new BPMapping<>("Canada", new PageOffsetReference(5L, 77)));
        BPInternalNode<String> internalNode = new BPInternalNode<>(4, placeHolderKeys);

        // we can imagine tree logic would insert pointers correctly
        internalNode.appendChildPointer(pointer3); // "Brazil" (P0)
        internalNode.appendChildPointer(pointer2); // "Liberia" (P1)
        internalNode.appendChildPointer(pointer0); // "Nigeria" (P2)
        internalNode.appendChildPointer(pointer1); // "UK" (P3)

        // we'll be passing the pointer object in this method - first delete should fail we didn't insert
        int result0 = internalNode.removePointer(pointer4);
        assertEquals(-1, result0);
        assertEquals(4, internalNode.getChildNodes());

        // deleting Brazil will shift all other pointers
        String result1Array = "[Liberia, Nigeria, United Kingdom]";
        int result1 = internalNode.removePointer(pointer3);
        assertEquals(0, result1);
        assertEquals(result1Array, createChildPointersArray(internalNode));
        assertEquals(3, internalNode.getChildNodes());

        String result2Array = "[Liberia, United Kingdom]";
        int result2 = internalNode.removePointer(pointer0);
        assertEquals(1, result2);
        assertEquals(result2Array, createChildPointersArray(internalNode));
        assertEquals(2, internalNode.getChildNodes());

        // deleting a pointer node that's already been deleted should fail
        int result3 = internalNode.removePointer(pointer3);
        assertEquals(-1, result3);
        assertEquals(result2Array, createChildPointersArray(internalNode));
        assertEquals(2, internalNode.getChildNodes());
    }


    @Test
    void isLacking() {
        Long[] placeHolderKeys = {1L, 2L, 3L, 4L};
        // we are only testing single part of internal node so we isolate this to testing child nodes
        BPLeafNode<Long> childNode0 = new BPLeafNode<>(5, new BPMapping<>(1L, new PageOffsetReference(1L, 20)));
        BPLeafNode<Long> childNode1 = new BPLeafNode<>(5, new BPMapping<>(2L, new PageOffsetReference(2L, 40)));
        BPLeafNode<Long> childNode2 = new BPLeafNode<>(5, new BPMapping<>(3L, new PageOffsetReference(3L, 50)));
        BPLeafNode<Long> childNode3 = new BPLeafNode<>(5, new BPMapping<>(4L, new PageOffsetReference(4L, 60)));
        BPLeafNode<Long> childNode4 = new BPLeafNode<>(5, new BPMapping<>(5L, new PageOffsetReference(5L, 70)));

        // internal node min child nodes = ceil(m/2) = 3
        BPInternalNode<Long> internalParentNode = new BPInternalNode<>(5, placeHolderKeys);

        //insert two nodes and check childNodes count and validate it's lacking
        boolean insert0 = internalParentNode.insertChildPointerAtIndex(childNode0, 0);
        boolean insert1 = internalParentNode.insertChildPointerAtIndex(childNode1, 1);

        assertAll(
                () -> assertTrue(insert0),
                () -> assertTrue(insert1),
                () -> assertTrue(internalParentNode.isLacking()),
                () -> assertEquals(2, internalParentNode.getChildNodes())
        );

        // 3rd insert means childNodes count = min, and we aren't lacking the minimum child nodes
        boolean insert2 = internalParentNode.insertChildPointerAtIndex(childNode2, 1);
        boolean insert3 = internalParentNode.insertChildPointerAtIndex(childNode3, 0);
        boolean insert4 = internalParentNode.insertChildPointerAtIndex(childNode4, 2);

        assertAll(
                () -> assertTrue(insert2),
                () -> assertTrue(insert3),
                () -> assertTrue(insert4),
                () -> assertFalse(internalParentNode.isLacking()),
                () -> assertEquals(5, internalParentNode.getChildNodes())
        );
    }

    @Test
    void isShareable() {
    }

    @Test
    void isMergeable() {
    }

    @Test
    void isOverfull() {
    }

    @Test
    void getMinChildNodes() {
        String[] keys = {"Ubuntu", "Debian", "Fedora", "Arch Linux", "Manjaro", "openSUSE", "RHEL", "centOS", "AlmaLinux",
                "Rocky Linux", "Kali Linux", "Parrot OS", "Zorin OS", "Linux Mint"
        };
        BPInternalNode<String> internalNode = new BPInternalNode<>(15, keys);
        int minChildNodes = (int) Math.ceil(15 / 2.0); // 8
        assertEquals(minChildNodes, internalNode.getMinChildNodes());
    }

    @Test
    void getMaxChildNodes() {
        String[] keys = {"Ubuntu", "Debian", "Fedora", "Arch Linux", "Manjaro", "openSUSE", "RHEL", "centOS", "AlmaLinux",
                "Rocky Linux", "Kali Linux", "Parrot OS", "Zorin OS", "Linux Mint"
        };
        BPInternalNode<String> internalNode = new BPInternalNode<>(15, keys);

        // max child nodes is equal to m
        assertEquals(15, internalNode.getMaxChildNodes());
    }

    @Test
    void getChildNodes() {
    }

    /**
     * The tree that is being created in these more involved node tests (siblings tests) and later in
     * Tree tests is the first example tree in this course paper.
     *
     * See the course documentation here: credit to CS 186, Spring 2021, Jenny Huang:
     * <a href="https://cs186berkeley.net/sp21/resources/static/notes/n03-B%2BTrees.pdf">LINK</a>
     */
    @Test
    @SuppressWarnings("unchecked")
    void checkInternalNodeSiblings() {
        Long[] rootKeys = {17L, null};
        Long[] leftInternalNodeKeys = {5L, 14L};
        Long[] rightInternalNodeKeys = {27L, null};

        BPNode<Long>[] internalPointers1 = new BPNode[]{null, null, null};
        BPInternalNode<Long> internalNode1 = new BPInternalNode<>(3, leftInternalNodeKeys, internalPointers1, SortOrder.ASC);

        BPNode<Long>[] internalPointers2 = new BPNode[]{null, null};
        BPInternalNode<Long> internalNode2 = new BPInternalNode<>(3, rightInternalNodeKeys, internalPointers2, SortOrder.ASC);

        // setSiblings
        internalNode1.setRightSibling(internalNode2);
        internalNode2.setLeftSibling(internalNode1);

        BPNode<Long>[] rootPointers = new BPNode[]{internalNode1, internalNode2};
        BPInternalNode<Long> rootNode = new BPInternalNode<>(3, rootKeys, rootPointers, SortOrder.ASC);

        // let's check siblings of internal node
        BPInternalNode<Long>[] rootNodeChildPointers = safeCastToInternalNodeArray(rootNode.getChildPointers());

        // left pointer is 0 indexed - check keys
        assertEquals(leftInternalNodeKeys, rootNodeChildPointers[0].getKeys());
        assertEquals(rootNodeChildPointers[0], internalNode2.getLeftSibling());

        // right pointer is 1 indexed - check keys
        assertEquals(rightInternalNodeKeys, rootNodeChildPointers[1].getKeys());
        assertEquals(rootNodeChildPointers[1], internalNode1.getRightSibling());
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable<T>> BPInternalNode<T>[] safeCastToInternalNodeArray(BPNode<T>[] nodes) {
        return Arrays.copyOf(nodes, nodes.length, BPInternalNode[].class);
    }

    @Test
    void getKeysIllegalArgumentException() {
        String expected1 = "Order of tree is 100. Key length must be 99, but key is null.";
        Exception actual1 = assertThrows(IllegalArgumentException.class, () -> new BPInternalNode<>(100, null));
        assertEquals(expected1, actual1.getMessage());
    }

    @Test
    void getKeys() {
        Integer[] keys = {200, 190, 180, 170, 160, 150, 140, 130, 120, 110, 100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0};
        BPInternalNode<Integer> internalNode = new BPInternalNode<>(22, keys);
        String keysString = "[200, 190, 180, 170, 160, 150, 140, 130, 120, 110, 100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0]";
        assertEquals(keysString, Arrays.toString(internalNode.getKeys()));

        // what about key array with nulls - often key arrays will have null values and this is valid
        String[] nullKeys = {null, null, null};
        BPInternalNode<String> nulledInternalNode = new BPInternalNode<>(4, nullKeys);
        String nulledKeyString = "[null, null, null]";
        assertEquals(nulledKeyString, Arrays.toString(nulledInternalNode.getKeys()));
    }

    @Test
    void getChildPointers() {
    }
}