package com.crushdb.index;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BPLeafNodeTest {

    @Test
    void insert() {
        BPMapping<Long> bpMapping1 = new BPMapping<>(1L, new PageOffsetReference(10L, 100));
        BPMapping<Long> bpMapping2 = new BPMapping<>(2L, new PageOffsetReference(20L, 200));
        BPMapping<Long> bpMapping3 = new BPMapping<>(3L, new PageOffsetReference(30L, 300));
        BPMapping<Long> bpMapping4 = new BPMapping<>(4L, new PageOffsetReference(40L, 400));

        BPLeafNode<Long> leafNode  = new BPLeafNode<>(4, bpMapping1);
        boolean insert1 = leafNode.insert(bpMapping2);
        boolean insert2 = leafNode.insert(bpMapping3);
        boolean insert3 = leafNode.insert(bpMapping4);

         // Remember the b+ tree rules: order (m) in this test is 4. Meaning maxPairs (keys) is m - 1.
         // In this case, that's three. The leaf node starts with 1 key. So, the first two inserts are
         // successful and the final insert is false. This will trigger a split at some point, but for
         // now we're testing in small units. We can also test the min and max pairs based on the order.
        assertAll(
                () -> assertEquals(3, leafNode.getNumPairs()),
                () -> assertEquals(3, leafNode.getMaxPairs()),
                () -> assertEquals(1, leafNode.getMinPairs()),
                () -> assertTrue(insert1),
                () -> assertTrue(insert2),
                () -> assertFalse(insert3)
        );

        // keys are sorted in leaf nodes - which is a prominent feature of b+ tree. Let's ensure our
        // keys are indeed sorted. Sorts are conducted on insert. This, perhaps, takes some amount of
        // performance hit. Is there a better data structure to optimize this?
        for (int i = 1; i < leafNode.getBpMappings().length; i++) {
            assertEquals(i, leafNode.getBpMappings()[i-1].getKey());
        }
    }

    @Test
    void delete() {
    }

    @Test
    void linearSearch() {
    }

    @Test
    void isFull() {
    }

    @Test
    void isLacking() {
    }

    @Test
    void isSharable() {
    }

    @Test
    void isAppendable() {
    }
}