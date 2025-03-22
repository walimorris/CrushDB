package com.crushdb.index;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BPTreeTest {

    @Test
    void insert() {
        BPTree<String> tree = new BPTree<>(3, SortOrder.ASC);
        boolean insert0 = tree.insert("Apple", new PageOffsetReference(1L, 22));
        boolean insert1 = tree.insert("Grape", new PageOffsetReference(2L, 23));
        boolean insert2 = tree.insert("Orange", new PageOffsetReference(3L, 24));
        boolean insert3 = tree.insert("Banana", new PageOffsetReference(4L, 25));
        boolean insert4 = tree.insert("Pineapple", new PageOffsetReference(5L, 26));
        boolean insert5 = tree.insert("BlueBerry", new PageOffsetReference(6L, 27));
        boolean insert6 = tree.insert("StrawBerry", new PageOffsetReference(7L, 28));
        boolean insert7 = tree.insert("Pear", new PageOffsetReference(8L, 29));
        boolean insert8 = tree.insert("Kiwi", new PageOffsetReference(9L, 30));
//        boolean insert9 = tree.insert("Cherry", new PageOffsetReference(10L, 31));

        assertAll(
                () -> assertTrue(insert0),
                () -> assertTrue(insert1),
                () -> assertTrue(insert2),
                () -> assertTrue(insert3),
                () -> assertTrue(insert4),
                () -> assertTrue(insert5),
                () -> assertTrue(insert6),
                () -> assertTrue(insert7),
                () -> assertTrue(insert8)
//                () -> assertTrue(insert9)

        );
    }

    @Test
    void search() {
    }

    @Test
    void rangeSearch() {
    }
}