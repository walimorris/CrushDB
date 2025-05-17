package com.crushdb.queryengine;

import com.crushdb.bootstrap.DatabaseInitializer;
import com.crushdb.index.BPTreeIndexManager;
import com.crushdb.index.btree.SortOrder;
import com.crushdb.model.crate.CrateManager;
import com.crushdb.model.document.BsonType;
import com.crushdb.model.document.Document;
import com.crushdb.queryengine.executor.QueryExecutor;
import com.crushdb.queryengine.parser.QueryParser;
import com.crushdb.queryengine.planner.QueryPlanner;
import com.crushdb.storageengine.StorageEngine;
import com.crushdb.storageengine.journal.JournalManager;
import com.crushdb.storageengine.page.PageManager;
import com.crushdb.utils.FileUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryEngineTest {

    private static PageManager pageManager;
    private static BPTreeIndexManager indexManager;
    private static JournalManager journalManager;
    private static CrateManager crateManager;
    private static Properties properties;

    private static QueryEngine queryEngine;

    private static Document document1;
    private static Document document2;
    private static Document document3;
    private static Document document4;

    @BeforeAll
    public static void setUp() {
        // storage engine
        FileUtil.destroyTestDatabaseDirectory();

        PageManager.reset();
        BPTreeIndexManager.reset();
        JournalManager.reset();
        CrateManager.reset();

        properties = DatabaseInitializer.init(true);

        pageManager = PageManager.getInstance(properties);
        indexManager = BPTreeIndexManager.getInstance(properties);
        journalManager = JournalManager.getInstance(properties);

        StorageEngine storageEngine = new StorageEngine(pageManager, indexManager, journalManager);

        // crate manager
        CrateManager.init(storageEngine);
        crateManager = CrateManager.getInstance(properties);

        // query engine
        QueryParser queryParser = new QueryParser();
        QueryPlanner queryPlanner = new QueryPlanner(crateManager);
        QueryExecutor queryExecutor = new QueryExecutor();
        queryEngine = new QueryEngine(queryParser, queryPlanner, queryExecutor);

        // documents
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

        // create indexes
        crateManager.createCrate("Vehicle");
        crateManager.getCrate("Vehicle").createIndex(BsonType.STRING, "vehicleMake_index", "vehicleMake", false, 3, SortOrder.ASC);

        // insert documents
        crateManager.getCrate("Vehicle").insert(document1);
        crateManager.getCrate("Vehicle").insert(document2);
        crateManager.getCrate("Vehicle").insert(document3);
        crateManager.getCrate("Vehicle").insert(document4);
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
    void find() {
        Map<String, Object> query1 = Map.of("vehicleMake", Map.of("$eq", "Subaru"));
        Map<String, Object> query2 = Map.of("vehicleMake", Map.of("$eq", "BMW"));
        Map<String, Object> query3 = Map.of("vehicleMake", Map.of("$eq", "Tesla"));
        List<Document> results1 = queryEngine.find("Vehicle", query1);
        List<Document> results2 = queryEngine.find("Vehicle", query2);
        List<Document> results3 = queryEngine.find("Vehicle", query3);

        assertEquals(2, results1.size());
        assertEquals(1, results2.size());
        assertEquals(1, results3.size());
    }
}