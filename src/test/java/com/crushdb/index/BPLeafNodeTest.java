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

        BPLeafNode<Long> leafNode  = new BPLeafNode<>(4, bpMapping3);
        boolean insert1 = leafNode.insert(bpMapping1);
        boolean insert2 = leafNode.insert(bpMapping4);
        boolean insert3 = leafNode.insert(bpMapping2);

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
        assertAll(
                () -> assertEquals(bpMapping1, leafNode.getBpMappings()[0]),
                () -> assertEquals(bpMapping3, leafNode.getBpMappings()[1]),
                () -> assertEquals(bpMapping4, leafNode.getBpMappings()[2])
        );
    }

    @Test
    void delete() {
        BPMapping<Long> bpMapping1 = new BPMapping<>(10L, new PageOffsetReference(1L, 32));
        BPMapping<Long> bpMapping2 = new BPMapping<>(20L, new PageOffsetReference(2L, 64));
        BPMapping<Long> bpMapping3 = new BPMapping<>(30L, new PageOffsetReference(3L, 128));
        BPMapping<Long> bpMapping4 = new BPMapping<>(40L, new PageOffsetReference(4L, 256));

        // max keys is m - 1 (3)
        BPLeafNode<Long> leafNode = new BPLeafNode<>(4, bpMapping3);

        boolean insert1 = leafNode.insert(bpMapping1);
        boolean insert2 = leafNode.insert(bpMapping4);
        boolean insert3 = leafNode.insert(bpMapping2);

        // trying to insert m (4) keys should fail rules of b+tree
        assertTrue(insert1);
        assertTrue(insert2);
        assertFalse(insert3);

        leafNode.delete(0);

        // after deleting a single key at index 0, we should have 2 keys remaining
        // index[0] should be null and the remainder key slots should be occupied
        assertAll(
                () -> assertEquals(2, leafNode.getNumPairs()),
                () -> assertNotNull(leafNode.getBpMappings()[0]),
                () -> assertNotNull(leafNode.getBpMappings()[1]),
                () -> assertNull(leafNode.getBpMappings()[2])
        );

        // deletion merges immediately, check correct values and CORRECT ORDER (inserts are in order)
        assertEquals(bpMapping3, leafNode.getBpMappings()[0]);
        leafNode.delete(0);
        assertEquals(bpMapping4, leafNode.getBpMappings()[0]);

        // deleting another key should bear 2 null indexes (1, 2) and a single
        // key remaining at index 0
        assertAll(
                () -> assertEquals(1, leafNode.getNumPairs()),
                () -> assertNull(leafNode.getBpMappings()[1]),
                () -> assertNull(leafNode.getBpMappings()[2]),
                () -> assertNotNull(leafNode.getBpMappings()[0])
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void linearSearch() {
        // let's create a simple linear search test. A leaf node with max keys of 6 will be created.
        // the BPMapping[] will contain keys at every index but the last, letting the client know
        // at which index the next node can be inserted
        BPMapping<Long> bpMapping1 = new BPMapping<>(10L, new PageOffsetReference(1L, 32));
        BPMapping<Long> bpMapping2 = new BPMapping<>(20L, new PageOffsetReference(2L, 64));
        BPMapping<Long> bpMapping3 = new BPMapping<>(30L, new PageOffsetReference(3L, 128));
        BPMapping<Long> bpMapping4 = new BPMapping<>(40L, new PageOffsetReference(4L, 256));
        BPMapping<Long> bpMapping5 = new BPMapping<>(50L, new PageOffsetReference(5L, 512));
        BPMapping<Long> bpMapping6 = new BPMapping<>(60L, new PageOffsetReference(6L, 1024));

        BPMapping<Long>[] mappings = (BPMapping<Long>[]) new BPMapping[]{
                bpMapping1, bpMapping2, bpMapping3, bpMapping4,
                bpMapping5, bpMapping6, null
        };
        BPLeafNode<Long> leafNode = new BPLeafNode<>(7, mappings, null);
        assertEquals(6, leafNode.linearSearch(leafNode.getBpMappings()));
    }

    @Test
    void isFull() {
       BPMapping<String> bpMapping1 = new BPMapping<>("United States", new PageOffsetReference(100L, 100));
       BPMapping<String> bpMapping2 = new BPMapping<>("Columbia", new PageOffsetReference(200L, 200));

       // max keys = 2 - initially contains 1 key
       BPLeafNode<String> leafNode = new BPLeafNode<>(3, bpMapping1);
       assertFalse(leafNode.isFull());

       leafNode.insert(bpMapping2);
       assertTrue(leafNode.isFull());
    }

    @Test
    void isLacking() {
        BPMapping<String> bpMapping1 = new BPMapping<>("United States", new PageOffsetReference(100L, 100));
        BPMapping<String> bpMapping2 = new BPMapping<>("United States", new PageOffsetReference(100L, 100));

        // max keys = 3 - initially contains 1 key and must have minimum 1 ceil(3/2) - 1
        BPLeafNode<String> leafNode = new BPLeafNode<>(4, bpMapping1);
        assertFalse(leafNode.isLacking());

        // remove key to make leaf node contain 0 keys, which is lacking min number of keys
        boolean delete1 = leafNode.delete(0);
        assertTrue(delete1);
        assertTrue(leafNode.isLacking());

        boolean insert1 = leafNode.insert(bpMapping1);
        boolean insert2 = leafNode.insert(bpMapping2);

        assertTrue(insert1);
        assertTrue(insert2);

        // deleting at index 2 should throw exception
        Exception exception = assertThrows(IndexOutOfBoundsException.class, () -> leafNode.delete(4));
        String expected = "Failure to delete key mapping on index: " + 4;
        String actual = exception.getMessage();
        assertEquals(expected, actual);
    }

    @Test
    void isSharable() {
    }

    @Test
    void isAppendable() {
    }
}