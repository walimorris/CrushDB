package com.crushdb.index;

import com.crushdb.bootstrap.DatabaseInitializer;
import com.crushdb.index.btree.*;
import com.crushdb.model.document.BsonType;
import com.crushdb.model.document.Document;
import com.crushdb.storageengine.page.Page;
import com.crushdb.utils.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BPTreeTest {
    private static BPTreeIndexManager indexManager;
    private static Properties properties;

    @BeforeEach
    public void setUp() {
        // Managers, such as the BPTreeIndexManager need to be reset because they retain in-memory state across
        // tests. With this, we can explicitly reset these manager.
        FileUtil.destroyTestDatabaseDirectory();
        BPTreeIndexManager.reset();
        properties = DatabaseInitializer.init(true);
        indexManager = BPTreeIndexManager.getInstance(properties);
    }

    @AfterEach
    public void tearDown() {
        BPTreeIndexManager.reset();
        FileUtil.destroyTestDatabaseDirectory();
    }

    @Test
    @SuppressWarnings("unchecked")
    void insertUniqueASC() {
        indexManager = BPTreeIndexManager.getInstance(properties);
        indexManager.createIndex(BsonType.STRING, "Food","fruit_index", "fruit_name", true, 3, SortOrder.ASC);
        BPTreeIndex<String> fruitIndex = (BPTreeIndex<String>) indexManager.getIndex("Food", "fruit_index");

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
    @SuppressWarnings("unchecked")
    void searchUniqueASC() {
        indexManager = BPTreeIndexManager.getInstance(properties);
        indexManager.createIndex(BsonType.STRING, "Food", "fruit_index", "fruit_name",true, 3, SortOrder.ASC);
        BPTreeIndex<String> fruitIndex = (BPTreeIndex<String>) indexManager.getIndex("Food", "fruit_index");

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
    void deepSearchUniqueASC() {
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
        System.out.println("ASC Deep Insert & Search: ");
        tree.printLeafNodes();
    }

    @Test
    @SuppressWarnings("unchecked")
    void searchUniqueWithDuplicateASC() {
        indexManager = BPTreeIndexManager.getInstance(properties);
        indexManager.createIndex(BsonType.STRING, "Food", "fruit_index", "fruit_name",true, 3, SortOrder.ASC);
        BPTreeIndex<String> fruitIndex = (BPTreeIndex<String>) indexManager.getIndex("Food", "fruit_index");

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
    @SuppressWarnings("unchecked")
    void searchNonUniqueASC() {
        // non-unique indexes can have multiple references with the same indexed key, in this case the
        // return is a list of references that point to the actual documents
        indexManager = BPTreeIndexManager.getInstance(properties);
        indexManager.createIndex(BsonType.STRING, "Food", "fruit_index", "fruit_name", false, 3, SortOrder.ASC);
        BPTreeIndex<String> fruitIndex = (BPTreeIndex<String>) indexManager.getIndex("Food", "fruit_index");

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
    @SuppressWarnings("unchecked")
    void rangeSearchASC() {
        indexManager = BPTreeIndexManager.getInstance(properties);
        indexManager.createIndex(BsonType.STRING, "Travel", "country_index", "country", false, 3, SortOrder.ASC);
        BPTreeIndex<String> countryIndex = (BPTreeIndex<String>) indexManager.getIndex("Travel", "country_index");

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

    @Test
    @SuppressWarnings("unchecked")
    void insertUniqueDESC() {
        indexManager = BPTreeIndexManager.getInstance(properties);
        indexManager.createIndex(BsonType.STRING, "Food", "fruit_index", "fruit_name",  true, 3, SortOrder.DESC);
        BPTreeIndex<String> fruitIndex = (BPTreeIndex<String>) indexManager.getIndex("Food", "fruit_index");

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
    @SuppressWarnings("unchecked")
    void searchUniqueDESC() {
        indexManager = BPTreeIndexManager.getInstance(properties);
        indexManager.createIndex(BsonType.STRING, "Food", "fruit_index", "fruit_name", true, 5, SortOrder.DESC);
        BPTreeIndex<String> fruitIndex = (BPTreeIndex<String>) indexManager.getIndex("Food", "fruit_index");

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

        BPTree<String> tree = fruitIndex.getTree();
        BPLeafNode<String> initialLeafNode = tree.getInitialLeafNode();

        // DESC order should be StrawBerry, Pineapple, Peer, Orange, Kiwi, Grape, Cherry, BlueBerry, Banana, Apple
        String[] expectedLeafValues = {"StrawBerry", "Pineapple", "Pear", "Orange", "Kiwi", "Grape", "Cherry", "BlueBerry", "Banana", "Apple"};
        String[] actualLeafValues = new String[10];

        // TODO: extract this into a method for other tests
        BPLeafNode<String> current = initialLeafNode;
        int i = 0;
        while (current != null) {
            BPMapping<String>[] mappings = current.getBpMappings();
            for (BPMapping<String> stringBPMapping : mappings) {
                if (stringBPMapping != null) {
                    String mapping = stringBPMapping.getKey();
                    actualLeafValues[i] = mapping;
                    i++;
                }
            }
            current = current.getRightSibling();
        }
        for (int j = 0; j < expectedLeafValues.length; j++) {
            assertEquals(expectedLeafValues[j], actualLeafValues[j]);
        }
    }

    @Test
    void deepSearchUniqueDESC() {
        BPTree<Long> tree = new BPTree<>(55, SortOrder.DESC);

        List<Long> numbers = new ArrayList<>();
        for (long i = 1; i <= 10000; i++) {
            numbers.add(i);
        }

        // reverse qupid shuffle!
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
        System.out.println("DESC Deep Insert & Search: ");
        tree.printLeafNodes();
    }

    @Test
    void searchNonUniqueASCDocuments() {
        // in the index manager we should probable have some schema parsing tool that can parse the
        // values to inject the index data type on creation
        indexManager = BPTreeIndexManager.getInstance(properties);
        BPTreeIndex<String> vehicleMakeIndex = indexManager.createIndex(BsonType.STRING, "Cars", "vehicle_make_index", "vehicleMake", false, 3, SortOrder.ASC);

        Page page = new Page(1L);

        Document document1 = new Document(1234567L);
        document1.put("vehicleMake", "Subaru");
        document1.put("vehicleModel", "Forester");
        document1.put("vehicleYear", 2019);
        document1.put("vehicleType", "automobile");
        document1.put("vehicleBodyStyle", "SUV");
        document1.put("vehiclePrice", 28500.99);
        document1.put("hasHeating", true);

        Document document2 = new Document(12345678L);
        document2.put("vehicleMake", "Subaru");
        document2.put("vehicleModel", "Impreza");
        document2.put("vehicleYear", 2018);
        document2.put("vehicleType", "automobile");
        document2.put("vehicleBodyStyle", "Sedan");
        document2.put("vehiclePrice", 22500.99);
        document2.put("hasHeating", true);

        Document document3 = new Document(123456789L);
        document3.put("vehicleMake", "Tesla");
        document3.put("vehicleModel", "Model 3");
        document3.put("vehicleYear", 2017);
        document3.put("vehicleType", "automobile");
        document3.put("vehicleBodyStyle", "Sedan");
        document3.put("vehiclePrice", 40200.99);
        document3.put("hasHeating", true);

        Document document4 = new Document(12345678910L);
        document4.put("vehicleMake", "BMW");
        document4.put("vehicleModel", "X3");
        document4.put("vehicleYear", 2014);
        document4.put("vehicleType", "automobile");
        document4.put("vehicleBodyStyle", "SUV");
        document4.put("vehiclePrice", 18000.00);
        document4.put("hasHeating", true);

        page.insertDocument(document1);
        page.insertDocument(document2);
        page.insertDocument(document3);
        page.insertDocument(document4);

        System.out.println(document1.getOffset());
        System.out.println(document2.getOffset());
        System.out.println(document3.getOffset());
        System.out.println(document4.getOffset());

        // Documents need to be added to page to get pageId and offset
        IndexEntry<String> indexEntry1 = IndexEntryBuilder.fromDocument(document1, vehicleMakeIndex);
        IndexEntry<String> indexEntry2 = IndexEntryBuilder.fromDocument(document2, vehicleMakeIndex);
        IndexEntry<String> indexEntry3 = IndexEntryBuilder.fromDocument(document3, vehicleMakeIndex);
        IndexEntry<String> indexEntry4 = IndexEntryBuilder.fromDocument(document4, vehicleMakeIndex);

        boolean insert0 = vehicleMakeIndex.insert(indexEntry1);
        boolean insert1 = vehicleMakeIndex.insert(indexEntry2);
        boolean insert2 = vehicleMakeIndex.insert(indexEntry3);
        boolean insert3 = vehicleMakeIndex.insert(indexEntry4);

        assertAll(
                () -> assertTrue(insert0),
                () -> assertTrue(insert1),
                () -> assertTrue(insert2),
                () -> assertTrue(insert3)
        );

        List<PageOffsetReference> result1 = vehicleMakeIndex.search(indexEntry1.key());
        List<PageOffsetReference> result2 = vehicleMakeIndex.search(indexEntry2.key());
        List<PageOffsetReference> result3 = vehicleMakeIndex.search(indexEntry3.key());
        List<PageOffsetReference> result4 = vehicleMakeIndex.search(indexEntry4.key());

        assertAll(
                // we can search the keys in the IndexEntry, ofcourse "Subaru" will return 2 values
                // because behicle_make was indexed. In the query layer, we can introduce filters
                () -> assertEquals(2, result1.size()),
                () -> assertEquals(2, result2.size()),
                () -> assertEquals(1, result3.size()),
                () -> assertEquals(1, result4.size())
        );

        // this is raw page manipulation: generally the PageManager will handle this, however the PageManager
        // has yet to be implemented. PageManager can retrieve Page, from page we can retrieve the Document.
        // For testing purposes we just want to ensure correctness at this stage
        assertEquals(page.getPageId(), result1.get(0).getPageId());
        assertEquals(page.getPageId(), result1.get(1).getPageId());
        assertEquals(page.getPageId(), result2.get(0).getPageId());
        assertEquals(page.getPageId(), result3.get(0).getPageId());

        Document finalDocument3 = page.readDocumentAtOffset(result3.get(0).getOffset());
        Document finalDocument4 = page.readDocumentAtOffset(result4.get(0).getOffset());

        assertEquals(document3.getFields(), finalDocument3.getFields());
        assertEquals(document4.getFields(), finalDocument4.getFields());

        BPTree<?> tree = vehicleMakeIndex.getTree();
        tree.printLeafNodes();
    }
}