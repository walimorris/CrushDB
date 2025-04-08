package com.crushdb.storageengine;

import com.crushdb.index.BPTreeIndex;
import com.crushdb.index.BPTreeIndexManager;
import com.crushdb.index.btree.SortOrder;
import com.crushdb.model.document.BsonType;
import com.crushdb.model.document.BsonValue;
import com.crushdb.model.document.Document;
import com.crushdb.storageengine.page.PageManager;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StorageEngineTest {
    private static StorageEngine storageEngine;
    private static PageManager pageManager = PageManager.getInstance();
    private static BPTreeIndexManager indexManager = BPTreeIndexManager.getInstance();
    private static Document document1;
    private static Document document2;
    private static Document document3;
    private static Document document4;

    @BeforeAll
    public static void setUp() {
        storageEngine = new StorageEngine(pageManager, indexManager);

        document1 = new Document(1234567L);
        document1.put("vehicle_make", "Subaru");
        document1.put("vehicle_model", "Forester");
        document1.put("vehicle_year", 2019);
        document1.put("vehicle_type", "automobile");
        document1.put("vehicle_body_style", "SUV");
        document1.put("vehicle_price", 28500.99);
        document1.put("hasHeating", true);

        document2 = new Document(12345678L);
        document2.put("vehicle_make", "Subaru");
        document2.put("vehicle_model", "Impreza");
        document2.put("vehicle_year", 2018);
        document2.put("vehicle_type", "automobile");
        document2.put("vehicle_body_style", "Sedan");
        document2.put("vehicle_price", 22500.99);
        document2.put("hasHeating", true);

        document3 = new Document(123456789L);
        document3.put("vehicle_make", "Tesla");
        document3.put("vehicle_model", "Model 3");
        document3.put("vehicle_year", 2017);
        document3.put("vehicle_type", "automobile");
        document3.put("vehicle_body_style", "Sedan");
        document3.put("vehicle_price", 40200.99);
        document3.put("hasHeating", true);

        document4 = new Document(12345678910L);
        document4.put("vehicle_make", "BMW");
        document4.put("vehicle_model", "X3");
        document4.put("vehicle_year", 2014);
        document4.put("vehicle_type", "automobile");
        document4.put("vehicle_body_style", "SUV");
        document4.put("vehicle_price", 18000.00);
        document4.put("hasHeating", true);
    }

    @Test
    @Order(1)
    void createIndex() {
        storageEngine.createIndex(BsonType.STRING, "Vehicle", "make_index", "vehicle_make", false, 3, SortOrder.ASC);
        storageEngine.createIndex(BsonType.LONG, "Vehicle", "id_index", "_id", false, 3, SortOrder.ASC);
        BPTreeIndexManager storageEngineIndexManager = storageEngine.indexManager();
        assertEquals(2, storageEngineIndexManager.getAllIndexesFromCrate("Vehicle").size());
        assertNotNull(storageEngineIndexManager.getIndex("Vehicle", "make_index"));
        assertNotNull(storageEngineIndexManager.getIndex("Vehicle", "id_index"));
    }

    @Test
    @Order(2)
    void insert() {
        Document result1 = storageEngine.insert(document1, "Vehicle");
        Document result2 = storageEngine.insert(document2, "Vehicle");
        Document result3 = storageEngine.insert(document3, "Vehicle");
        Document result4 = storageEngine.insert(document4, "Vehicle");

        assertAll(
                () -> assertNotNull(result1),
                () -> assertNotNull(result2),
                () -> assertNotNull(result3),
                () -> assertNotNull(result4),
                () -> assertEquals(document1, result1),
                () -> assertEquals(document2, result2),
                () -> assertEquals(document3, result3),
                () -> assertEquals(document4, result4)
        );
    }

    @Test
    @Order(3)
    void find() {
        // keep in mind, we are testing the storage engine. Crates will only pass the field and value and
        // the index corresponding index(es) will be found in the crate's find method before passing to
        // the storage engine
        BPTreeIndex<?> index = null;
        List<BPTreeIndex<?>> indexes = storageEngine.indexManager().getAllIndexesFromCrate("Vehicle");
        for (BPTreeIndex<?> i : indexes) {
            if (i.getIndexName().equals("make_index")) {
                index = i;
                break;
            }
        }

        BPTreeIndex<?> id_index = null;
        List<BPTreeIndex<?>> moreIndexes = storageEngine.indexManager().getAllIndexesFromCrate("Vehicle");
        for (BPTreeIndex<?> i : moreIndexes) {
            if (i.getIndexName().equals("id_index")) {
                id_index = i;
                break;
            }
        }

        List<Document> resultSet1 = storageEngine.find("Vehicle", index, BsonValue.ofString("Subaru"));
        List<Document> resultSet2 = storageEngine.find("Vehicle", index, BsonValue.ofString("BMW"));
        List<Document> resultSet3 = storageEngine.find("Vehicle", index, BsonValue.ofString("Tesla"));

        List<Document> resultSet4 = storageEngine.find("vehicle", id_index, BsonValue.ofLong(document1.getDocumentId()));

        assertAll(
                () -> assertEquals(2, resultSet1.size()),
                () -> assertEquals(1, resultSet2.size()),
                () -> assertEquals(1, resultSet3.size()),
                () -> assertEquals(1, resultSet4.size())
        );
    }

    @Test
    void rangeFind() {
        BPTreeIndex<?> index = null;
        List<BPTreeIndex<?>> indexes = storageEngine.indexManager().getAllIndexesFromCrate("Vehicle");
        for (BPTreeIndex<?> i : indexes) {
            if (i.getIndexName().equals("make_index")) {
                index = i;
                break;
            }
        }

        // range search is Acura -> Subaru (inclusive). This search will return and makes between these values - including BMW
        List<Document> result = storageEngine.rangeFind("Vehicle", index, BsonValue.ofString("Acura"), BsonValue.ofString("Subaru"));
        assertEquals(3, result.size());
        for (Document doc : result) {
            doc.prettyPrint();
        }
    }

    @Test
    void scan() {
    }
}