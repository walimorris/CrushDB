package com.crushdb.queryengine;

import com.crushdb.index.BPTreeIndexManager;
import com.crushdb.index.btree.SortOrder;
import com.crushdb.model.crate.CrateManager;
import com.crushdb.model.document.BsonType;
import com.crushdb.model.document.Document;
import com.crushdb.queryengine.executor.QueryExecutor;
import com.crushdb.queryengine.parser.QueryParser;
import com.crushdb.queryengine.planner.QueryPlanner;
import com.crushdb.storageengine.StorageEngine;
import com.crushdb.storageengine.page.PageManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryEngineTest {

    private static QueryEngine queryEngine;

    private static Document document1;
    private static Document document2;
    private static Document document3;
    private static Document document4;
    private static Document document5;

    @BeforeAll
    public static void setUp() {
        // storage engine
        PageManager pageManager = PageManager.getInstance();
        BPTreeIndexManager indexManager = BPTreeIndexManager.getInstance();
        StorageEngine storageEngine = new StorageEngine(pageManager, indexManager);

        // crate manager
        CrateManager.init(storageEngine);
        CrateManager crateManager = CrateManager.getInstance();

        // query engine
        QueryParser queryParser = new QueryParser();
        QueryPlanner queryPlanner = new QueryPlanner(crateManager);
        QueryExecutor queryExecutor = new QueryExecutor();
        queryEngine = new QueryEngine(queryParser, queryPlanner, queryExecutor);

        // documents
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

        // create indexes
        crateManager.createCrate("Vehicle");
        crateManager.getCrate("Vehicle").createIndex(BsonType.STRING, "make_index", "vehicle_make", false, 3, SortOrder.ASC);
        crateManager.getCrate("Vehicle").createIndex(BsonType.LONG, "id_index", "_id", false, 3, SortOrder.ASC);

        // insert documents
        crateManager.getCrate("Vehicle").insert(document1);
        crateManager.getCrate("Vehicle").insert(document2);
        crateManager.getCrate("Vehicle").insert(document3);
        crateManager.getCrate("Vehicle").insert(document4);
    }

    @Test
    void find() {
        Map<String, Object> query1 = Map.of("vehicle_make", Map.of("$eq", "Subaru"));
        Map<String, Object> query2 = Map.of("vehicle_make", Map.of("$eq", "BMW"));
        Map<String, Object> query3 = Map.of("vehicle_make", Map.of("$eq", "Tesla"));
        List<Document> results1 = queryEngine.find("Vehicle", query1);
        List<Document> results2 = queryEngine.find("Vehicle", query2);
        List<Document> results3 = queryEngine.find("Vehicle", query3);

        assertEquals(2, results1.size());
        assertEquals(1, results2.size());
        assertEquals(1, results3.size());
    }
}