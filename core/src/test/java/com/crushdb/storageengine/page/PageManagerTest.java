package com.crushdb.storageengine.page;

import com.crushdb.bootstrap.CrushContext;
import com.crushdb.bootstrap.DatabaseInitializer;
import com.crushdb.index.btree.PageOffsetReference;
import com.crushdb.model.document.Document;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PageManagerTest {
    private static CrushContext cxt;
    private static Document document1;
    private static Document document2;
    private static Document document3;
    private static Document document4;

    @BeforeAll
    static void setup() {
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

    }

    @Test
    @Order(1)
    void getInstance() {
        assertNotNull(cxt.getPageManager());
    }

    @Test
    @Order(2)
    void insertAndRetrieveDocument() {
        Document result1 = cxt.getPageManager().insertDocument(document1);
        Document result2 = cxt.getPageManager().insertDocument(document2);
        Document result3 = cxt.getPageManager().insertDocument(document3);
        Document result4 = cxt.getPageManager().insertDocument(document4);

        System.out.println(result1);
        System.out.println(result2);
        System.out.println(result3);
        System.out.println(result4);

        // generally the PageOffsetReference will come from getting the Documents
        // from a query reference and parsing these values. However, these test
        // just check that page manager operations work as they should
        PageOffsetReference pageOffsetReference1 = new PageOffsetReference(result1.getPageId(), result1.getOffset());
        PageOffsetReference pageOffsetReference2 = new PageOffsetReference(result2.getPageId(), result2.getOffset());
        PageOffsetReference pageOffsetReference3 = new PageOffsetReference(result3.getPageId(), result3.getOffset());
        PageOffsetReference pageOffsetReference4 = new PageOffsetReference(result4.getPageId(), result4.getOffset());

        Document retrievedDocument1 = cxt.getPageManager().retrieveDocument(pageOffsetReference1);
        Document retrievedDocument2 = cxt.getPageManager().retrieveDocument(pageOffsetReference2);
        Document retrievedDocument3 = cxt.getPageManager().retrieveDocument(pageOffsetReference3);
        Document retrievedDocument4 = cxt.getPageManager().retrieveDocument(pageOffsetReference4);

        assertEquals(document1, retrievedDocument1);
        assertEquals(document2, retrievedDocument2);
        assertEquals(document3, retrievedDocument3);
        assertEquals(document4, retrievedDocument4);
    }

    @Test
    @Order(3)
    void flushAll() {
        cxt.getPageManager().flushAll();
    }
}