package com.crushdb.storageengine.page;

import com.crushdb.index.btree.PageOffsetReference;
import com.crushdb.model.document.Document;
import org.junit.jupiter.api.*;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PageManagerTest {

    private static PageManager pageManager;
    private static Document document1;
    private static Document document2;
    private static Document document3;
    private static Document document4;

    @BeforeAll
    static void setup() {
        Properties properties = null;
        pageManager = PageManager.getInstance(properties);
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
    void getInstance() {
        assertNotNull(pageManager);
    }

    @Test
    @Order(2)
    void insertAndRetrieveDocument() {
        Document result1 = pageManager.insertDocument(document1);
        Document result2 = pageManager.insertDocument(document2);
        Document result3 = pageManager.insertDocument(document3);
        Document result4 = pageManager.insertDocument(document4);

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

        Document retrievedDocument1 = pageManager.retrieveDocument(pageOffsetReference1);
        Document retrievedDocument2 = pageManager.retrieveDocument(pageOffsetReference2);
        Document retrievedDocument3 = pageManager.retrieveDocument(pageOffsetReference3);
        Document retrievedDocument4 = pageManager.retrieveDocument(pageOffsetReference4);

        assertEquals(document1, retrievedDocument1);
        assertEquals(document2, retrievedDocument2);
        assertEquals(document3, retrievedDocument3);
        assertEquals(document4, retrievedDocument4);
    }

    @Test
    @Order(3)
    void flushAll() {
        pageManager.flushAll();
    }
}