package com.crushdb.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
        boolean insert9 = tree.insert("Cherry", new PageOffsetReference(10L, 31));

        assertAll(
                () -> assertTrue(insert0),
                () -> assertTrue(insert1),
                () -> assertTrue(insert2),
                () -> assertTrue(insert3),
                () -> assertTrue(insert4),
                () -> assertTrue(insert5),
                () -> assertTrue(insert6),
                () -> assertTrue(insert7),
                () -> assertTrue(insert8),
                () -> assertTrue(insert9)

        );
    }

    @Test
    void search() {
        BPTree<String> tree = new BPTree<>(3, SortOrder.ASC);
        tree.insert("Apple", new PageOffsetReference(1L, 22));
        tree.insert("Grape", new PageOffsetReference(2L, 23));
        tree.insert("Orange", new PageOffsetReference(3L, 24));
        tree.insert("Banana", new PageOffsetReference(4L, 25));
        tree.insert("Pineapple", new PageOffsetReference(5L, 26));
        tree.insert("BlueBerry", new PageOffsetReference(6L, 27));
        tree.insert("StrawBerry", new PageOffsetReference(7L, 28));
        tree.insert("Pear", new PageOffsetReference(8L, 29));
        tree.insert("Kiwi", new PageOffsetReference(9L, 30));
        tree.insert("Cherry", new PageOffsetReference(10L, 31));

        assertAll(
                () -> assertEquals(1L, tree.search("Apple").getPageId()),
                () -> assertEquals(2L, tree.search("Grape").getPageId()),
                () -> assertEquals(3L, tree.search("Orange").getPageId()),
                () -> assertEquals(4L, tree.search("Banana").getPageId()),
                () -> assertEquals(5L, tree.search("Pineapple").getPageId()),
                () -> assertEquals(6L, tree.search("BlueBerry").getPageId()),
                () -> assertEquals(7L, tree.search("StrawBerry").getPageId()),
                () -> assertEquals(8L, tree.search("Pear").getPageId()),
                () -> assertEquals(9L, tree.search("Kiwi").getPageId()),
                () -> assertEquals(10L, tree.search("Cherry").getPageId())
        );
    }

    @Test
    void deepSearch() {
        BPTree<Long> tree = new BPTree<>(55, SortOrder.ASC);

        List<Long> numbers = new ArrayList<>();
        for (long i = 1; i <= 10000; i++) {
            numbers.add(i);
        }

        // qupid shuffle!
        Collections.shuffle(numbers);

        for (long number : numbers) {
            boolean inserted = tree.insert(number, new PageOffsetReference(number, (int) number + 10000));
            assertTrue(inserted, "Insert failed at: " + number);
        }
        List<Executable> assertions = new ArrayList<>();
        for (long i = 1; i <= 10000; i++) {
            long finalI = i;
            assertions.add(() -> {
                PageOffsetReference ref = tree.search(finalI);
                assertNotNull(ref, "search returned null for: " + finalI);
                assertEquals(finalI, ref.getPageId(), "PageId mismatch at: " + finalI);
            });
        }
        assertAll(assertions);
        tree.printLeafNodes();
    }

    @Test
    void searchWithDuplicate() {
        BPTree<String> tree = new BPTree<>(3, true, SortOrder.ASC);
        boolean insert0 = tree.insert("Apple", new PageOffsetReference(1L, 22));
        boolean insert1 = tree.insert("Grape", new PageOffsetReference(2L, 23));
        boolean insert2 = tree.insert("Orange", new PageOffsetReference(3L, 24));
        boolean insert3 = tree.insert("Banana", new PageOffsetReference(4L, 25));
        boolean insert4 = tree.insert("Pineapple", new PageOffsetReference(5L, 26));
        boolean insert5 = tree.insert("BlueBerry", new PageOffsetReference(6L, 27));
        boolean insert6 = tree.insert("StrawBerry", new PageOffsetReference(7L, 28));
        boolean insert7 = tree.insert("Pear", new PageOffsetReference(8L, 29));
        boolean insert8 = tree.insert("Kiwi", new PageOffsetReference(9L, 30));
        boolean insert9 = tree.insert("Cherry", new PageOffsetReference(10L, 31));

        assertAll(
                () -> assertTrue(insert0),
                () -> assertTrue(insert1),
                () -> assertTrue(insert2),
                () -> assertTrue(insert3),
                () -> assertTrue(insert4),
                () -> assertTrue(insert5),
                () -> assertTrue(insert6),
                () -> assertTrue(insert7),
                () -> assertTrue(insert8),
                () -> assertTrue(insert9)
        );
        assertThrows(DuplicateKeyException.class, () -> tree.insert("Pineapple", new PageOffsetReference(77L, 777)));
        assertThrows(DuplicateKeyException.class, () -> tree.insert("StrawBerry", new PageOffsetReference(717L, 577)));
        assertThrows(DuplicateKeyException.class, () -> tree.insert("Grape", new PageOffsetReference(123L, 67)));

        assertAll(
                () -> assertEquals(1L, tree.search("Apple").getPageId()),
                () -> assertEquals(2L, tree.search("Grape").getPageId()),
                () -> assertEquals(3L, tree.search("Orange").getPageId()),
                () -> assertEquals(4L, tree.search("Banana").getPageId()),
                () -> assertEquals(5L, tree.search("Pineapple").getPageId()),
                () -> assertEquals(6L, tree.search("BlueBerry").getPageId()),
                () -> assertEquals(7L, tree.search("StrawBerry").getPageId()),
                () -> assertEquals(8L, tree.search("Pear").getPageId()),
                () -> assertEquals(9L, tree.search("Kiwi").getPageId()),
                () -> assertEquals(10L, tree.search("Cherry").getPageId())
        );
    }

    @Test
    void rangeSearch() {
    }
}