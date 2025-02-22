package com.crushdb.storage;

import com.crushdb.model.Document;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PageTest {


    @Test
    public void pageDocumentDecompressedInsertAndReadTest() {
        long pageId = 1L;
        Page page = new Page(pageId, false);

        Document doc1 = new Document(1);
        Document doc2 = new Document(2);
        Document doc3 = new Document(3);

        doc1.put("name", "Michael");
        doc1.put("age", "62");
        doc1.put("profession", "Basketball");

        doc2.put("programming_language", "Java");

        doc3.put("favorite_food", "Spaghetti");

        page.insertDocument(doc1);
        page.insertDocument(doc2);
        page.insertDocument(doc3);

        Document retrievedDoc1 = page.retrieveDocument(doc1.getDocumentId());
        Document retrievedDoc2 = page.retrieveDocument(doc2.getDocumentId());
        Document retrievedDoc3 = page.retrieveDocument(doc3.getDocumentId());

        assertNotNull(retrievedDoc1);
        assertNotNull(retrievedDoc2);
        assertNotNull(retrievedDoc3);

        assertAll(
                () -> assertEquals("Michael", retrievedDoc1.get("name")),
                () ->  assertEquals("62", retrievedDoc1.get("age")),
                () -> assertEquals("Basketball", retrievedDoc1.get("profession")),
                () -> assertEquals("Java", retrievedDoc2.get("programming_language")),
                () -> assertEquals("Spaghetti", retrievedDoc3.get("favorite_food"))
        );
    }

    @Test
    public void pageDocumentCompressedInsertAndReadTest() {
        long pageId = 2L;
        Page page = new Page(pageId, true);

        Document document1 = new Document(1);
        document1.put("Database", "MongoDB");
        document1.put("DatabaseType", "Document");
        document1.put("DataType", "Bson");

        Document document2 = new Document(2);
        document2.put("company_name", "Apple");
        document2.put("company_industry", "Information Technology");

        page.insertDocument(document1);
        page.insertDocument(document2);

        Document retrievedDoc1 = page.retrieveDocument(document1.getDocumentId());
        Document retrievedDoc2 = page.retrieveDocument(document2.getDocumentId());
        assertNotNull(retrievedDoc1);
        assertNotNull(retrievedDoc2);
    }
}