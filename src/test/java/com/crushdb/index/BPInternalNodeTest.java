package com.crushdb.index;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BPInternalNodeTest {

    @Test
    void insertChildPointerAtIndex() {
        // child nodes are equal to order of tree(m), however the data structure can hold
        // m + 1 child nodes to account for splits
        Long[] keys = {12L, 13L, 14L};

        // we will point to other internal nodes
        BPInternalNode<Long> childNode1 = new BPInternalNode<>(4, null);
        BPInternalNode<Long> childNode2 = new BPInternalNode<>(4, null);
        BPInternalNode<Long> childNode3 = new BPInternalNode<>(4, null);
        BPInternalNode<Long> childNode4 = new BPInternalNode<>(4, null);
        BPInternalNode<Long> childNode5 = new BPInternalNode<>(4, null);
        BPInternalNode<Long> childNode6 = new BPInternalNode<>(4, null);

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
        boolean insert5 = internalNode.insertChildPointerAtIndex(childNode5, internalNode.getChildNodes());

        // this calls insert on the max child value (5) - however this is out-of-bounds for zero indexing
        // in this case, overFull should deny this insert
        boolean insert6 = internalNode.insertChildPointerAtIndex(childNode6, internalNode.getChildNodes());

        assertAll(
                () -> assertTrue(insert4),
                () -> assertTrue(insert5),
                () -> assertFalse(insert6),
                () -> assertTrue(internalNode.isOverfull()),
                () -> assertEquals(childNode4, internalNode.getChildPointers()[3]),
                // we can ensure that the last node inserted is the max
                () -> assertEquals(childNode5, internalNode.getChildPointers()[internalNode.getMaxChildNodes()])
        );
    }

    @Test
    void findChildPointerIndex() {
        BPInternalNode<String> pointer1 = new BPInternalNode<>(3, null);
        BPInternalNode<String> pointer2 = new BPInternalNode<>(3, null);
        BPInternalNode<String> pointer3 = new BPInternalNode<>(3, null);

        String[] keys = {"United States", "Peru"};

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
        BPInternalNode<String> pointer1 = new BPInternalNode<>(3, null);
        BPInternalNode<String> pointer2 = new BPInternalNode<>(3, null);
        BPInternalNode<String> pointer3 = new BPInternalNode<>(3, null);
        BPInternalNode<String> pointer4 = new BPInternalNode<>(3, null);
        BPInternalNode<String> pointer5 = new BPInternalNode<>(3, null);

        BPInternalNode<String> parent = new BPInternalNode<>(3, null);
        boolean insert1 = parent.insertChildPointerAtIndex(pointer4, 0);
        boolean insert2 = parent.appendChildPointer(pointer2);
        boolean insert3 = parent.appendChildPointer(pointer1);
        boolean insert4 = parent.appendChildPointer(pointer3);

        // max is 4 child-child pointers, after which the node is overfull
        boolean insert5 = parent.appendChildPointer(pointer5);

        // append insert order -> pointer4, pointer2, pointer1, pointer3
        assertAll(
                () -> assertTrue(insert1),
                () -> assertTrue(insert2),
                () -> assertTrue(insert3),
                () -> assertTrue(insert4),
                () -> assertFalse(insert5),
                () -> assertEquals(pointer4, parent.getChildPointers()[0]),
                () -> assertEquals(pointer2, parent.getChildPointers()[1]),
                () -> assertEquals(pointer1, parent.getChildPointers()[2]),
                // max = 4
                () -> assertEquals(pointer3, parent.getChildPointers()[parent.getMaxChildNodes()]),
                // max nodes is 4(m), however overfull will be max + 1 (zero indexed) or current number of child nodes
                () -> assertEquals(pointer3, parent.getChildPointers()[parent.getChildNodes() - 1])
        );
    }

    @Test
    void prependChildPointer() {
        // let's point to a leaf node - bp internal node and bp leaf node extends bp node
        BPLeafNode<Long> pointer1 = new BPLeafNode<>(3, new BPMapping<>(1L, new PageOffsetReference(10L, 36)));
        BPLeafNode<Long> pointer2 = new BPLeafNode<>(3, new BPMapping<>(2L, new PageOffsetReference(20L, 72)));
        BPLeafNode<Long> pointer3 = new BPLeafNode<>(3, new BPMapping<>(3L, new PageOffsetReference(30L, 144)));
        BPLeafNode<Long> pointer4 = new BPLeafNode<>(3, new BPMapping<>(4L, new PageOffsetReference(60L, 288)));

        // max = 2, pointersMax = 3 - max pointer index will be index[2]
        BPInternalNode<Long> parent = new BPInternalNode<>(2, null);
        boolean insert1 = parent.insertChildPointerAtIndex(pointer1, 0);
        boolean insert2 = parent.prependChildPointer(pointer3);
        boolean insert3 = parent.prependChildPointer(pointer2);
        boolean insert4 = parent.prependChildPointer(pointer4);

        // we've insert 3 nodes, this is overfull
        assertAll(
                () -> assertTrue(parent.isOverfull()),
                () -> assertTrue(insert1),
                () -> assertTrue(insert2),
                () -> assertTrue(insert3),
                () -> assertFalse(insert4)
        );

        // the max child nodes should be 2(m) - however to account for splitting we leave space
        // for one more node pointer, so the current number of child nodes should be the max of 3
        assertEquals(3, parent.getChildNodes());
        assertEquals(2, parent.getMaxChildNodes());

        // we can count on the latest inserts being the earliest in the pointers array
        assertAll(
                () -> assertEquals(pointer2, parent.getChildPointers()[0]),
                () -> assertEquals(pointer3, parent.getChildPointers()[1]),
                // we can count on the last pointer being the limit (account for zero index) and last valid insert
                () -> assertEquals(pointer1, parent.getChildPointers()[parent.getMaxChildNodes()])
        );

        // let's visualize this
        String result = "[2, 3, 1]";
        StringBuilder resultString = new StringBuilder();
        resultString.append("[");
        for (int i = 0; i < parent.getChildPointers().length; i++) {
            BPLeafNode<Long> leafNode = (BPLeafNode<Long>) parent.getChildPointers()[i];
            if (i == parent.getMaxChildNodes()) {
                resultString.append(leafNode.getBpMappings()[0].getKey());
            } else {
                resultString.append(leafNode.getBpMappings()[0].getKey()).append(", ");
            }
        }
        resultString.append("]");
        assertEquals(result, resultString.toString());
    }

    @Test
    void removeKeyAtIndex() {
    }

    @Test
    void removePointerAtIndex() {
    }

    @Test
    void removePointer() {
    }

    @Test
    void isLacking() {
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
    }

    @Test
    void getMaxChildNodes() {
    }

    @Test
    void getChildNodes() {
    }

    @Test
    void getLeftSibling() {
    }

    @Test
    void getRightSibling() {
    }

    @Test
    void getKeys() {
    }

    @Test
    void getChildPointers() {
    }
}