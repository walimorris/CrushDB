package com.crushdb.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BPTreeTest {

    @Test
    void insertUnique() {
        BPTreeIndexManager<String> indexManager = new BPTreeIndexManager<>();
        indexManager.createIndex("fruit_index", true, 3, SortOrder.ASC);
        BPTreeIndex<String> fruitIndex = indexManager.getIndex("fruit_index");

        assertAll(
                () -> assertTrue(fruitIndex.insert("Apple", new PageOffsetReference(1L, 22))),
                () -> assertTrue(fruitIndex.insert("Grape", new PageOffsetReference(2L, 23))),
                () -> assertTrue(fruitIndex.insert("Orange", new PageOffsetReference(3L, 24))),
                () -> assertTrue(fruitIndex.insert("Banana", new PageOffsetReference(4L, 25))),
                () -> assertTrue(fruitIndex.insert("Pineapple", new PageOffsetReference(5L, 26))),
                () -> assertTrue(fruitIndex.insert("BlueBerry", new PageOffsetReference(6L, 27))),
                () -> assertTrue(fruitIndex.insert("StrawBerry", new PageOffsetReference(7L, 28))),
                () -> assertTrue(fruitIndex.insert("Pear", new PageOffsetReference(8L, 29))),
                () -> assertTrue(fruitIndex.insert("Kiwi", new PageOffsetReference(9L, 30))),
                () -> assertTrue(fruitIndex.insert("Cherry", new PageOffsetReference(10L, 31)))
        );
    }

    @Test
    void searchUnique() {
        BPTreeIndexManager<String> indexManager = new BPTreeIndexManager<>();
        indexManager.createIndex("fruit_index", true, 3, SortOrder.ASC);
        BPTreeIndex<String> fruitIndex = indexManager.getIndex("fruit_index");

        fruitIndex.insert("Apple", new PageOffsetReference(1L, 22));
        fruitIndex.insert("Grape", new PageOffsetReference(2L, 23));
        fruitIndex.insert("Orange", new PageOffsetReference(3L, 24));
        fruitIndex.insert("Banana", new PageOffsetReference(4L, 25));
        fruitIndex.insert("Pineapple", new PageOffsetReference(5L, 26));
        fruitIndex.insert("BlueBerry", new PageOffsetReference(6L, 27));
        fruitIndex.insert("StrawBerry", new PageOffsetReference(7L, 28));
        fruitIndex.insert("Pear", new PageOffsetReference(8L, 29));
        fruitIndex.insert("Kiwi", new PageOffsetReference(9L, 30));
        fruitIndex.insert("Cherry", new PageOffsetReference(10L, 31));

        assertAll(
                () -> assertEquals(1L, fruitIndex.search("Apple").get(0).getPageId()),
                () -> assertEquals(2L, fruitIndex.search("Grape").get(0).getPageId()),
                () -> assertEquals(3L, fruitIndex.search("Orange").get(0).getPageId()),
                () -> assertEquals(4L, fruitIndex.search("Banana").get(0).getPageId()),
                () -> assertEquals(5L, fruitIndex.search("Pineapple").get(0).getPageId()),
                () -> assertEquals(6L, fruitIndex.search("BlueBerry").get(0).getPageId()),
                () -> assertEquals(7L, fruitIndex.search("StrawBerry").get(0).getPageId()),
                () -> assertEquals(8L, fruitIndex.search("Pear").get(0).getPageId()),
                () -> assertEquals(9L, fruitIndex.search("Kiwi").get(0).getPageId()),
                () -> assertEquals(10L, fruitIndex.search("Cherry").get(0).getPageId())
        );
    }

    @Test
    void deepSearchUnique() {
        BPTree<Long> tree = new BPTree<>(55, SortOrder.ASC);

        List<Long> numbers = new ArrayList<>();
        for (long i = 1; i <= 10000; i++) {
            numbers.add(i);
        }

        // qupid shuffle!
        Collections.shuffle(numbers);

        for (long number : numbers) {
            boolean inserted = tree.insert(number, new PageOffsetReference(number, (int) number + 10000), true);
            assertTrue(inserted, "Insert failed at: " + number);
        }
        List<Executable> assertions = new ArrayList<>();
        for (long i = 1; i <= 10000; i++) {
            long finalI = i;
            assertions.add(() -> {
                PageOffsetReference ref = tree.search(finalI).get(0);
                assertNotNull(ref, "search returned null for: " + finalI);
                assertEquals(finalI, ref.getPageId(), "PageId mismatch at: " + finalI);
            });
        }
        assertAll(assertions);
        tree.printLeafNodes();
    }

    @Test
    void searchUniqueWithDuplicate() {
        BPTreeIndexManager<String> indexManager = new BPTreeIndexManager<>();
        indexManager.createIndex("fruit_index", true, 3, SortOrder.ASC);
        BPTreeIndex<String> fruitIndex = indexManager.getIndex("fruit_index");

        boolean insert0 = fruitIndex.insert("Apple", new PageOffsetReference(1L, 22));
        boolean insert1 = fruitIndex.insert("Grape", new PageOffsetReference(2L, 23));
        boolean insert2 = fruitIndex.insert("Orange", new PageOffsetReference(3L, 24));
        boolean insert3 = fruitIndex.insert("Banana", new PageOffsetReference(4L, 25));
        boolean insert4 = fruitIndex.insert("Pineapple", new PageOffsetReference(5L, 26));
        boolean insert5 = fruitIndex.insert("BlueBerry", new PageOffsetReference(6L, 27));
        boolean insert6 = fruitIndex.insert("StrawBerry", new PageOffsetReference(7L, 28));
        boolean insert7 = fruitIndex.insert("Pear", new PageOffsetReference(8L, 29));
        boolean insert8 = fruitIndex.insert("Kiwi", new PageOffsetReference(9L, 30));
        boolean insert9 = fruitIndex.insert("Cherry", new PageOffsetReference(10L, 31));

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

        // inserting on unique indexes should fail with duplicate key error
        assertThrows(DuplicateKeyException.class, () -> fruitIndex.insert("Pineapple", new PageOffsetReference(77L, 777)));
        assertThrows(DuplicateKeyException.class, () -> fruitIndex.insert("StrawBerry", new PageOffsetReference(717L, 577)));
        assertThrows(DuplicateKeyException.class, () -> fruitIndex.insert("Grape", new PageOffsetReference(123L, 67)));

        assertAll(
                () -> assertEquals(1L, fruitIndex.search("Apple").get(0).getPageId()),
                () -> assertEquals(2L, fruitIndex.search("Grape").get(0).getPageId()),
                () -> assertEquals(3L, fruitIndex.search("Orange").get(0).getPageId()),
                () -> assertEquals(4L, fruitIndex.search("Banana").get(0).getPageId()),
                () -> assertEquals(5L, fruitIndex.search("Pineapple").get(0).getPageId()),
                () -> assertEquals(6L, fruitIndex.search("BlueBerry").get(0).getPageId()),
                () -> assertEquals(7L, fruitIndex.search("StrawBerry").get(0).getPageId()),
                () -> assertEquals(8L, fruitIndex.search("Pear").get(0).getPageId()),
                () -> assertEquals(9L, fruitIndex.search("Kiwi").get(0).getPageId()),
                () -> assertEquals(10L, fruitIndex.search("Cherry").get(0).getPageId())
        );
    }

    @Test
    void searchNonUnique() {
        // non-unique indexes can have multiple references with the same indexed key, in this case the
        // return is a list of references that point to the actual documents
        BPTreeIndexManager<String> indexManager = new BPTreeIndexManager<>();
        indexManager.createIndex("fruit_index", false, 3, SortOrder.ASC);
        BPTreeIndex<String> fruitIndex = indexManager.getIndex("fruit_index");

        // in the case of documents, they will have an indexed field(s) and can contain different fields
        // this test case simply tests for records with the same indexed key, however different references
        boolean insert0 = fruitIndex.insert("Apple", new PageOffsetReference(1L, 22));
        boolean insert1 = fruitIndex.insert("Grape", new PageOffsetReference(2L, 23));
        boolean insert2 = fruitIndex.insert("Orange", new PageOffsetReference(3L, 24));
        boolean insert3 = fruitIndex.insert("Banana", new PageOffsetReference(4L, 25));
        boolean insert4 = fruitIndex.insert("Pineapple", new PageOffsetReference(5L, 26));
        boolean insert5 = fruitIndex.insert("BlueBerry", new PageOffsetReference(6L, 27));
        boolean insert6 = fruitIndex.insert("StrawBerry", new PageOffsetReference(7L, 28));
        boolean insert7 = fruitIndex.insert("Pear", new PageOffsetReference(8L, 29));
        boolean insert8 = fruitIndex.insert("Kiwi", new PageOffsetReference(9L, 30));
        boolean insert9 = fruitIndex.insert("Cherry", new PageOffsetReference(10L, 31));
        boolean insert10 = fruitIndex.insert("Apple", new PageOffsetReference(10L, 222));
        boolean insert11 = fruitIndex.insert("Grape", new PageOffsetReference(20L, 233));
        boolean insert12 = fruitIndex.insert("Orange", new PageOffsetReference(30L, 244));
        boolean insert13 = fruitIndex.insert("Banana", new PageOffsetReference(40L, 255));
        boolean insert14 = fruitIndex.insert("Pineapple", new PageOffsetReference(50L, 266));
        boolean insert15 = fruitIndex.insert("BlueBerry", new PageOffsetReference(60L, 277));
        boolean insert16 = fruitIndex.insert("StrawBerry", new PageOffsetReference(70L, 288));
        boolean insert17 = fruitIndex.insert("Pear", new PageOffsetReference(80L, 299));
        boolean insert18 = fruitIndex.insert("Kiwi", new PageOffsetReference(90L, 300));
        boolean insert19 = fruitIndex.insert("Cherry", new PageOffsetReference(100L, 311));

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
                () -> assertTrue(insert9),
                () -> assertTrue(insert10),
                () -> assertTrue(insert11),
                () -> assertTrue(insert12),
                () -> assertTrue(insert13),
                () -> assertTrue(insert14),
                () -> assertTrue(insert15),
                () -> assertTrue(insert16),
                () -> assertTrue(insert17),
                () -> assertTrue(insert18),
                () -> assertTrue(insert19)
        );

        assertAll(
                () -> assertEquals(2, fruitIndex.search("Apple").size()),
                () -> assertEquals(2, fruitIndex.search("Grape").size()),
                () -> assertEquals(2, fruitIndex.search("Orange").size()),
                () -> assertEquals(2, fruitIndex.search("Banana").size()),
                () -> assertEquals(2, fruitIndex.search("Pineapple").size()),
                () -> assertEquals(2, fruitIndex.search("BlueBerry").size()),
                () -> assertEquals(2, fruitIndex.search("StrawBerry").size()),
                () -> assertEquals(2, fruitIndex.search("Pear").size()),
                () -> assertEquals(2, fruitIndex.search("Kiwi").size()),
                () -> assertEquals(2, fruitIndex.search("Cherry").size())
        );
    }

    @Test
    void rangeSearch() {
        BPTreeIndexManager<String> indexManager = new BPTreeIndexManager<>();
        indexManager.createIndex("country_index", false, 3, SortOrder.ASC);
        BPTreeIndex<String> countryIndex = indexManager.getIndex("country_index");

        boolean insert0 = countryIndex.insert("United States", new PageOffsetReference(1L, 22));
        boolean insert1 = countryIndex.insert("United Kingdom", new PageOffsetReference(2L, 23));
        boolean insert2 = countryIndex.insert("Kenya", new PageOffsetReference(3L, 24));
        boolean insert3 = countryIndex.insert("Brazil", new PageOffsetReference(4L, 25));
        boolean insert4 = countryIndex.insert("Barbados", new PageOffsetReference(5L, 26));
        boolean insert5 = countryIndex.insert("Chile", new PageOffsetReference(6L, 27));
        boolean insert6 = countryIndex.insert("Denmark", new PageOffsetReference(7L, 28));
        boolean insert7 = countryIndex.insert("Finland", new PageOffsetReference(8L, 29));
        boolean insert8 = countryIndex.insert("Germany", new PageOffsetReference(9L, 20));
        boolean insert9 = countryIndex.insert("Barbados", new PageOffsetReference(10L, 72));
        boolean insert10 = countryIndex.insert("Denmark", new PageOffsetReference(14L, 56));

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
                () -> assertTrue(insert9),
                () -> assertTrue(insert10)
        );

        Map<String, List<PageOffsetReference>> references = countryIndex.rangeSearch("B", "H");

        // nice! we can do letter searches for range
        assertEquals(6, references.size());

        // the expected results
        List<String> results = List.of("Brazil", "Barbados", "Chile", "Denmark", "Finland", "Germany");

        // expect the above six keys
        for (String key : references.keySet()) {
            assertTrue(results.contains(key));
        }

        // expect the keys that contain multiple page offsets to contain the expected number
        assertEquals(2, references.get("Barbados").size());
        assertEquals(2, references.get("Denmark").size());
    }
}