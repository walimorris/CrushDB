package com.crushdb.core.storageengine;

import com.crushdb.core.bootstrap.CrushContext;
import com.crushdb.core.bootstrap.DatabaseInitializer;
import com.crushdb.core.index.BPTreeIndex;
import com.crushdb.core.index.BPTreeIndexManager;
import com.crushdb.core.index.btree.SortOrder;
import com.crushdb.core.model.crate.CrateManager;
import com.crushdb.core.model.document.BsonType;
import com.crushdb.core.model.document.BsonValue;
import com.crushdb.core.model.document.Document;
import com.crushdb.core.storageengine.journal.JournalManager;
import com.crushdb.core.storageengine.page.PageManager;
import com.crushdb.core.utils.FileUtil;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StorageEngineTest {
    private static CrushContext cxt;
    private static Document document1;
    private static Document document2;
    private static Document document3;
    private static Document document4;
    private static Document document5;

    @BeforeAll
    public static void setUp() {
        FileUtil.destroyTestDatabaseDirectory();

        PageManager.reset();
        BPTreeIndexManager.reset();
        JournalManager.reset();
        CrateManager.reset();

        cxt = DatabaseInitializer.initTest();

        document1 = new Document(1234567L);
        document1.put("vehicleMake", "Subaru");
        document1.put("vehicleModel", "Forester");
        document1.put("vehicleYear", 2019);
        document1.put("vehicleType", "automobile");
        document1.put("vehicleBodyStyle", "SUV");
        document1.put("vehiclePrice", 28500.99);
        document1.put("hasHeating", true);

        document2 = new Document(12345678L);
        document2.put("vehicleMake", "Subaru");
        document2.put("vehicleModel", "Impreza");
        document2.put("vehicleYear", 2018);
        document2.put("vehicleType", "automobile");
        document2.put("vehicleBodyStyle", "Sedan");
        document2.put("vehiclePrice", 22500.99);
        document2.put("hasHeating", true);

        document3 = new Document(123456789L);
        document3.put("vehicleMake", "Tesla");
        document3.put("vehicleModel", "Model 3");
        document3.put("vehicleYear", 2017);
        document3.put("vehicleType", "automobile");
        document3.put("vehicleBodyStyle", "Sedan");
        document3.put("vehiclePrice", 40200.99);
        document3.put("hasHeating", true);

        document4 = new Document(12345678910L);
        document4.put("vehicleMake", "BMW");
        document4.put("vehicleModel", "X3");
        document4.put("vehicleYear", 2014);
        document4.put("vehicleType", "automobile");
        document4.put("vehicleBodyStyle", "SUV");
        document4.put("vehiclePrice", 18000.00);
        document4.put("hasHeating", true);

        document5 = new Document(987654321L);
        document5.put("deviceModel", "Raspberry Pi");
        document5.put("deviceName", "Locust");
        document5.put("deviceSerialNumber", 232345455L);
    }

    @AfterAll
    public static void tearDown() {
        FileUtil.destroyTestDatabaseDirectory();
        PageManager.reset();
        BPTreeIndexManager.reset();
        JournalManager.reset();
        CrateManager.reset();
    }

    @Test
    @Order(1)
    void createIndex() {
        cxt.getStorageEngine().createIndex(BsonType.STRING, "Vehicle", "make_index", "vehicleMake", false, 3, SortOrder.ASC);
        cxt.getStorageEngine().createIndex(BsonType.LONG, "Vehicle", "id_index", "_id", false, 3, SortOrder.ASC);
        BPTreeIndexManager storageEngineIndexManager = cxt.getStorageEngine().getIndexManager();
        assertEquals(2, storageEngineIndexManager.getAllIndexesFromCrate("Vehicle").size());
        assertNotNull(storageEngineIndexManager.getIndex("Vehicle", "make_index"));
        assertNotNull(storageEngineIndexManager.getIndex("Vehicle", "id_index"));
    }

    @Test
    @Order(2)
    void insert() {
        Document result1 = cxt.getStorageEngine().insert("Vehicle", document1);
        Document result2 = cxt.getStorageEngine().insert("Vehicle", document2);
        Document result3 = cxt.getStorageEngine().insert("Vehicle", document3);
        Document result4 = cxt.getStorageEngine().insert("Vehicle", document4);

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
        List<BPTreeIndex<?>> indexes = cxt.getStorageEngine().getIndexManager().getAllIndexesFromCrate("Vehicle");
        for (BPTreeIndex<?> i : indexes) {
            if (i.getIndexName().equals("make_index")) {
                index = i;
                break;
            }
        }

        BPTreeIndex<?> id_index = null;
        List<BPTreeIndex<?>> moreIndexes = cxt.getStorageEngine().getIndexManager().getAllIndexesFromCrate("Vehicle");
        for (BPTreeIndex<?> i : moreIndexes) {
            if (i.getIndexName().equals("id_index")) {
                id_index = i;
                break;
            }
        }

        List<Document> resultSet1 = cxt.getStorageEngine().find("Vehicle", index, BsonValue.ofString("Subaru"));
        List<Document> resultSet2 = cxt.getStorageEngine().find("Vehicle", index, BsonValue.ofString("BMW"));
        List<Document> resultSet3 = cxt.getStorageEngine().find("Vehicle", index, BsonValue.ofString("Tesla"));

        List<Document> resultSet4 = cxt.getStorageEngine().find("vehicle", id_index, BsonValue.ofLong(document1.getDocumentId()));

        assertAll(
                () -> assertEquals(2, resultSet1.size()),
                () -> assertEquals(1, resultSet2.size()),
                () -> assertEquals(1, resultSet3.size()),
                () -> assertEquals(1, resultSet4.size())
        );
    }

    @Test
    @Order(4)
    void rangeFind() {
        BPTreeIndex<?> index = null;
        List<BPTreeIndex<?>> indexes = cxt.getStorageEngine().getIndexManager().getAllIndexesFromCrate("Vehicle");
        for (BPTreeIndex<?> i : indexes) {
            if (i.getIndexName().equals("make_index")) {
                index = i;
                break;
            }
        }

        // range search is Acura -> Subaru (inclusive). This search will return and makes between these values - including BMW
        List<Document> result = cxt.getStorageEngine().rangeFind("Vehicle", index, BsonValue.ofString("Acura"), BsonValue.ofString("Subaru"));
        assertEquals(3, result.size());
        for (Document doc : result) {
            doc.prettyPrint();
        }
    }

//    @Test
//    void scan() {
//        Crate crate = new Crate("Devices", storageEngine);
//        Document resultDocument = storageEngine.insert(crate.getName(), document5);
//        List<Document> scanResult = storageEngine.scan(crate.getName(), "device_model", BsonValue.ofString("Raspberry Pi"));
//        // at this point the document is in memory because it has been recently inserted, but what about when it gets flushed?
//        // the storage engine will need to, not only search memory, but then search db file on disk. Which makes the scan very
//        // expensive. In any case, the data will be found.
//        assertEquals(document5, resultDocument);
//        assertEquals(1, scanResult.size());
//        assertEquals(scanResult.get(0), document5);
//        scanResult.get(0).prettyPrint();
//    }
}