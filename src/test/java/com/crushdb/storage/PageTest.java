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

        doc1.put("name", "Jim");
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
                () -> assertEquals("Jim", retrievedDoc1.get("name")),
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


    @Test
    public void deleteUncompressedDocumentTest() {
        Page page = new Page(4L, false);

        Document document = new Document(12L);
        document.put("Cassandra", "Tombstone");
        page.insertDocument(document);

        Document retrieveDocument = page.retrieveDocument(document.getDocumentId());
        assertNotNull(retrieveDocument);
        assertEquals("Tombstone", document.get("Cassandra"));

        page.deleteDocument(document.getDocumentId());
        Document retrieveDeletedDocument = page.retrieveDocument(document.getDocumentId());
        assertNull(retrieveDeletedDocument);

        // let's see follow on inserts and retrievals - of course crushdb will implement defragmentation,
        // compaction, and page splits at some point but this will be good to check now that our offsets
        // are still being positioned and pulled correctly. At some point the page will be defragmented.
        Document document2 = new Document(15L);
        Document document3 = new Document(789L);
        document2.put("studentId", "01234567");
        document3.put("employeeId", "76543210");
        page.insertDocument(document2);
        page.insertDocument(document3);

        Document retrieveDocument2 = page.retrieveDocument(document2.getDocumentId());
        Document retrieveDocument3 = page.retrieveDocument(document3.getDocumentId());

        assertAll(
                () -> assertNotNull(retrieveDocument2),
                () -> assertNotNull(retrieveDocument3),
                () -> assertEquals("01234567", retrieveDocument2.get("studentId")),
                () -> assertEquals("76543210", retrieveDocument3.get("employeeId"))
        );
    }

    @Test
    public void compactUnCompressedPageTest() {
        Page page = new Page(23, false);

        Document document1 = new Document(1);
        Document document2 = new Document(2);
        Document document3 = new Document(3);
        Document document4 = new Document(4L);
        Document document5 = new Document(5L);

        document1.put("name", "jim");
        document1.put("age", "45");
        document2.put("name", "amber");
        document2.put("age", "32");
        document3.put("name", "kimberly");
        document3.put("age", "23");
        document4.put("name", "mike");
        document4.put("age", "26");
        document5.put("name", "sam");
        document5.put("age", "62");

        page.insertDocument(document1);
        page.insertDocument(document2);
        page.insertDocument(document3);
        page.insertDocument(document4);
        page.insertDocument(document5);

        Document retrievedDocument1 = page.retrieveDocument(document1.getDocumentId());
        Document retrievedDocument2 = page.retrieveDocument(document2.getDocumentId());
        Document retrievedDocument3 = page.retrieveDocument(document3.getDocumentId());
        Document retrievedDocument4 = page.retrieveDocument(document4.getDocumentId());
        Document retrievedDocument5 = page.retrieveDocument(document5.getDocumentId());

        assertAll(
                () -> assertNotNull(retrievedDocument1),
                () -> assertNotNull(retrievedDocument2),
                () -> assertNotNull(retrievedDocument3),
                () -> assertNotNull(retrievedDocument4),
                () -> assertNotNull(retrievedDocument5),
                () -> assertEquals("jim", retrievedDocument1.get("name")),
                () -> assertEquals("amber", retrievedDocument2.get("name")),
                () -> assertEquals("kimberly", retrievedDocument3.get("name")),
                () -> assertEquals("mike", retrievedDocument4.get("name")),
                () -> assertEquals("sam", retrievedDocument5.get("name"))
        );

        // now for deletes
        page.deleteDocument(document1.getDocumentId());
        page.deleteDocument(document2.getDocumentId());

        Document deletedDocument1 = page.retrieveDocument(document1.getDocumentId());
        Document deletedDocument2 = page.retrieveDocument(document2.getDocumentId());

        // expect null documents
        assertNull(deletedDocument1);
        assertNull(deletedDocument2);

        // expect not null document
        assertNotNull(page.retrieveDocument(document3.getDocumentId()));
        assertNotNull(page.retrieveDocument(document4.getDocumentId()));
        assertNotNull(page.retrieveDocument(document5.getDocumentId()));

        // Documents have been added with data and validated.
        // Documents have been deleted and valid.
        // Documents that weren't deleted remain and are validated.
        // This only tells us that the offsets are being read correctly in the page.
        // Now we will compact the page and attempt the same validations.
        boolean success = page.compactPage();
        assertTrue(success);

        Document compactedDocument3 = page.retrieveDocument(document3.getDocumentId());
        Document compactedDocument4 = page.retrieveDocument(document4.getDocumentId());
        Document compactedDocument5 = page.retrieveDocument(document5.getDocumentId());

        assertAll(
                () -> assertNotNull(compactedDocument3),
                () -> assertNotNull(compactedDocument4),
                () -> assertNotNull(compactedDocument5),
                () -> assertEquals("kimberly", compactedDocument3.get("name")),
                () -> assertEquals("mike", compactedDocument4.get("name")),
                () -> assertEquals("sam", compactedDocument5.get("name"))
        );
    }
}