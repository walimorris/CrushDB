package com.crushdb.storageengine.page;

import com.crushdb.model.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PageManagerTest {

    @Test
    void getInstance() {
        PageManager pageManager = PageManager.getInstance();
        assertNotNull(pageManager);
    }

    @Test
    void insertDocument() {
        PageManager pageManager = PageManager.getInstance();

        Document document1 = new Document(1234567L);
        document1.put("vehicle_make", "Subaru");
        document1.put("vehicle_model", "Forester");
        document1.put("vehicle_year", 2019);
        document1.put("vehicle_type", "automobile");
        document1.put("vehicle_body_style", "SUV");
        document1.put("vehicle_price", 28500.99);
        document1.put("hasHeating", true);

        Document document2 = new Document(12345678L);
        document2.put("vehicle_make", "Subaru");
        document2.put("vehicle_model", "Impreza");
        document2.put("vehicle_year", 2018);
        document2.put("vehicle_type", "automobile");
        document2.put("vehicle_body_style", "Sedan");
        document2.put("vehicle_price", 22500.99);
        document2.put("hasHeating", true);

        Document document3 = new Document(123456789L);
        document3.put("vehicle_make", "Tesla");
        document3.put("vehicle_model", "Model 3");
        document3.put("vehicle_year", 2017);
        document3.put("vehicle_type", "automobile");
        document3.put("vehicle_body_style", "Sedan");
        document3.put("vehicle_price", 40200.99);
        document3.put("hasHeating", true);

        Document document4 = new Document(12345678910L);
        document4.put("vehicle_make", "BMW");
        document4.put("vehicle_model", "X3");
        document4.put("vehicle_year", 2014);
        document4.put("vehicle_type", "automobile");
        document4.put("vehicle_body_style", "SUV");
        document4.put("vehicle_price", 18000.00);
        document4.put("hasHeating", true);

        Document result1 = pageManager.insertDocument(document1);
        Document result2 = pageManager.insertDocument(document2);
        Document result3 = pageManager.insertDocument(document3);
        Document result4 = pageManager.insertDocument(document4);

        System.out.println(result1);
        System.out.println(result2);
        System.out.println(result3);
        System.out.println(result4);
    }

    @Test
    void retrieveDocument() {
    }

    @Test
    void flushAll() {
    }

    @Test
    void flush() {
    }
}