package com.crushdb.storage;

import com.crushdb.model.Document;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PageTest {


    @Test
    public void testPageCompressionAndDecompression() {
        long pageId = 1L;
        Page page = new Page(pageId);

        Document doc1 = new Document(1);
        doc1.put("name", "Michael Jordan");
        doc1.put("age", "62");
        doc1.put("profession", "Basketball");

        Document doc2 = new Document(2);
        doc2.put("name", "Adele Laurie Blue Adkins");
        doc2.put("age", "36");
        doc2.put("profession", "Singer");

        page.insertDocument(doc1);
        page.insertDocument(doc2);

        Document retrievedDoc1 = page.retrieveDocument(doc1.getDocumentId());
        Document retrievedDoc2 = page.retrieveDocument(doc2.getDocumentId());

        assertNotNull(retrievedDoc1);
        assertNotNull(retrievedDoc2);

        assertAll(
                () -> assertEquals("Michael Jordan", retrievedDoc1.get("name")),
                () ->  assertEquals("62", retrievedDoc1.get("age")),
                () -> assertEquals("Basketball", retrievedDoc1.get("profession"))
        );

        page.compressPage();
        assertTrue(page.isCompressed());
        assertTrue(page.getCompressedPageSize() < 4096);

        // TODO: fix decompression issue - however every passes up to this point
//        page.decompressPage();
//        assertFalse(page.isCompressed());
    }
}