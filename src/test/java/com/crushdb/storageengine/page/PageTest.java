package com.crushdb.storageengine.page;

import com.crushdb.model.Document;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

        Document retrievedDocument1 = page.insertDocument(doc1);
        Document retrievedDocument2 = page.insertDocument(doc2);
        Document retrievedDocument3 = page.insertDocument(doc3);

        assertNotNull(retrievedDocument1);
        assertNotNull(retrievedDocument2);
        assertNotNull(retrievedDocument3);

        assertAll(
                () -> assertEquals("Jim", retrievedDocument1.get("name")),
                () ->  assertEquals("62", retrievedDocument1.get("age")),
                () -> assertEquals("Basketball", retrievedDocument1.get("profession")),
                () -> assertEquals("Java", retrievedDocument2.get("programming_language")),
                () -> assertEquals("Spaghetti", retrievedDocument3.get("favorite_food")),
                () -> assertEquals(1, retrievedDocument1.getPageId()),
                () -> assertEquals(1, retrievedDocument2.getPageId()),
                () -> assertEquals(1, retrievedDocument3.getPageId())
        );
    }

    @Test
    public void pageDocumentCompressedInsertAndReadTest() {
        Page page = new Page(2L, true);

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

        assertAll(
                () -> assertEquals("MongoDB", retrievedDoc1.get("Database")),
                () ->  assertEquals("Bson", retrievedDoc1.get("DataType"))
        );

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
        boolean success = (boolean) page.compactPage().get("state");
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

    @Test
    public void compactCompressedPageTest() {
        System.out.println("PlaceHolder");
    }

    @Test
    public void decompressCompressedPage() {
        // regardless if using compressPage(), when autoCompressOnInsert is on, pages are compressed
        Page page = new Page(1L, true);
        Document document1 = new Document(1);
        Document document2 = new Document(2);
        Document document3 = new Document(3);

        document1.put("name", "jim");
        document1.put("compressed", "true");

        document2.put("autoCompressOnInsert", "true");
        document2.put("maxPageSize", "4096");

        document3.put("maxHeaderSize", "32");
        document3.put("documentSize", "variable");

        page.insertDocument(document1);
        page.insertDocument(document2);
        page.insertDocument(document3);

        Document retrieveDocument1 = page.retrieveDocument(document1.getDocumentId());
        Document retrieveDocument2 = page.retrieveDocument(document2.getDocumentId());
        Document retrieveDocument3 = page.retrieveDocument(document3.getDocumentId());

        assertAll(
                () -> assertEquals("jim", retrieveDocument1.get("name")),
                () -> assertEquals("true", retrieveDocument1.get("compressed")),
                () -> assertEquals("true", retrieveDocument2.get("autoCompressOnInsert")),
                () -> assertEquals("4096", retrieveDocument2.get("maxPageSize")),
                () -> assertEquals("32", retrieveDocument3.get("maxHeaderSize")),
                () -> assertEquals("variable", retrieveDocument3.get("documentSize"))
        );

        // page is compressed with autoCompressOnInsert, now for decompressing the whole page
        // this means, rather than decompressing individual pages, we decompress all documents
        // in the page in one go
        int calculatedCompressedSizePre = (page.getHeaderSize() +
                retrieveDocument1.getCompressedSize() +
                retrieveDocument2.getCompressedSize() +
                retrieveDocument3.getCompressedSize()
        );

        assertEquals(page.getPageSize(), calculatedCompressedSizePre);
        page.decompressPage();

        Document retrievedResult1 = page.retrieveDocument(retrieveDocument1.getDocumentId());
        Document retrievedResult2 = page.retrieveDocument(retrieveDocument2.getDocumentId());
        Document retrievedResult3 = page.retrieveDocument(retrieveDocument3.getDocumentId());

        assertAll(
                () -> assertEquals("jim", retrievedResult1.get("name")),
                () -> assertEquals("true", retrievedResult2.get("autoCompressOnInsert")),
                () -> assertEquals("32", retrievedResult3.get("maxHeaderSize"))
        );

        // assert all documents are decompressed
        int calculatedDecompressedSizePost = (page.getHeaderSize() +
                retrievedResult1.getDecompressedSize() +
                retrievedResult2.getDecompressedSize() +
                retrievedResult3.getDecompressedSize()
        );
        assertAll(
                () -> assertEquals(0, retrievedResult1.getCompressedSize() - 25),
                () -> assertEquals(0, retrievedResult2.getCompressedSize() - 25),
                () -> assertEquals(0, retrievedResult3.getCompressedSize() - 25),
                () -> assertEquals(4096, page.getAvailableSpace() + page.getPageSize()),
                () -> assertEquals(page.getPageSize(), calculatedDecompressedSizePost)
        );
    }

    @Test
    public void decompressDecompressedPage() {
        Page page = new Page(1L, false);
        Document document1 = new Document(1);
        Document document2 = new Document(2);
        Document document3 = new Document(3);

        document1.put("name", "jim");
        document1.put("compressed", "true");

        document2.put("autoCompressOnInsert", "true");
        document2.put("maxPageSize", "4096");

        document3.put("maxHeaderSize", "32");
        document3.put("documentSize", "variable");

        page.insertDocument(document1);
        page.insertDocument(document2);
        page.insertDocument(document3);

        Document retrieveDocument1 = page.retrieveDocument(document1.getDocumentId());
        Document retrieveDocument2 = page.retrieveDocument(document2.getDocumentId());
        Document retrieveDocument3 = page.retrieveDocument(document3.getDocumentId());

        assertAll(
                () -> assertEquals("jim", retrieveDocument1.get("name")),
                () -> assertEquals("true", retrieveDocument1.get("compressed")),
                () -> assertEquals("true", retrieveDocument2.get("autoCompressOnInsert")),
                () -> assertEquals("4096", retrieveDocument2.get("maxPageSize")),
                () -> assertEquals("32", retrieveDocument3.get("maxHeaderSize")),
                () -> assertEquals("variable", retrieveDocument3.get("documentSize"))
        );
        int calculatedDecompressedSizePre = (page.getHeaderSize() +
                retrieveDocument1.getDecompressedSize() +
                retrieveDocument2.getDecompressedSize() +
                retrieveDocument3.getDecompressedSize()
        );
        assertEquals(page.getPageSize(), calculatedDecompressedSizePre);

        // assertion is thrown trying to decompress an already decompressed page
        Exception exception = assertThrows(IllegalStateException.class, page::decompressPage);
        String expectedMessage = String.format("ERROR: Attempting to decompress page with ID '%d' that is already decompressed", page.getPageId());
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);

        Document retrievedResult1 = page.retrieveDocument(retrieveDocument1.getDocumentId());
        Document retrievedResult2 = page.retrieveDocument(retrieveDocument2.getDocumentId());
        Document retrievedResult3 = page.retrieveDocument(retrieveDocument3.getDocumentId());

        assertAll(
                () -> assertEquals("jim", retrievedResult1.get("name")),
                () -> assertEquals("true", retrievedResult2.get("autoCompressOnInsert")),
                () -> assertEquals("32", retrievedResult3.get("maxHeaderSize"))
        );

        // assert all documents are decompressed and correct size
        int calculatedDecompressedSizePost = (page.getHeaderSize() +
                retrievedResult1.getDecompressedSize() +
                retrievedResult2.getDecompressedSize() +
                retrievedResult3.getDecompressedSize()
        );
        assertAll(
                () -> assertEquals(0, retrievedResult1.getCompressedSize() - 25),
                () -> assertEquals(0, retrievedResult2.getCompressedSize() - 25),
                () -> assertEquals(0, retrievedResult3.getCompressedSize() - 25),
                () -> assertEquals(4096, page.getAvailableSpace() + page.getPageSize()),
                () -> assertEquals(page.getPageSize(), calculatedDecompressedSizePost)
        );
    }

    @Test
    public void splitDecompressedPage() {
        // compaction occurs so let's spice things up by adding documents, marking for
        // deletion, ensuring quality, ensuring quality compaction first
        Page page = new Page(99L, false);
        Document doc1 = new Document(1L);
        Document doc2 = new Document(2L);
        Document doc3 = new Document(3L);
        Document doc4 = new Document(4L);
        Document doc5 = new Document(5L);
        Document doc6 = new Document(6L);


        doc1.put("name", "james");
        doc1.put("hobby", "skating");

        doc2.put("name", "amber");
        doc2.put("hobby", "tennis");

        doc3.put("name", "sam");
        doc3.put("hobby", "piano");

        doc4.put("name", "viki");
        doc4.put("hobby", "art");

        doc5.put("name", "lee");
        doc5.put("hobby", "soccer");

        doc6.put("name", "mohan");
        doc6.put("hobby", "swimming");

        page.insertDocument(doc1);
        page.insertDocument(doc2);
        page.insertDocument(doc3);
        page.insertDocument(doc4);
        page.insertDocument(doc5);
        page.insertDocument(doc6);

        Document result1 = page.retrieveDocument(doc1.getDocumentId());
        Document result2 = page.retrieveDocument(doc2.getDocumentId());
        Document result3 = page.retrieveDocument(doc3.getDocumentId());
        Document result4 = page.retrieveDocument(doc4.getDocumentId());
        Document result5 = page.retrieveDocument(doc5.getDocumentId());
        Document result6 = page.retrieveDocument(doc6.getDocumentId());

        assertAll(
                () -> assertEquals("james", result1.get("name")),
                () -> assertEquals("amber", result2.get("name")),
                () -> assertEquals("sam", result3.get("name")),
                () -> assertEquals("viki", result4.get("name")),
                () -> assertEquals("lee", result5.get("name")),
                () -> assertEquals("mohan", result6.get("name")),
                () -> assertEquals(6, page.getNumberOfDocuments())
        );
        // delete document to create tombstone
        page.deleteDocument(doc2.getDocumentId());
        assertEquals(5, page.getNumberOfDocuments());

        // compact
        Map<String, Object> compactResult = page.compactPage();
        assertEquals(true, compactResult.get("state"));
        assertEquals(page, compactResult.get("page"));
        assertEquals(5, page.getNumberOfDocuments());

        // number of docs in new page
        int right = (int) Math.ceil(page.getNumberOfDocuments() * 0.5);
        assertEquals(3, right);

        // number of docs that stay in current page
        int left = page.getNumberOfDocuments() - right;
        assertEquals(2, left);

        PageSplitResult result = page.splitPage();
        long newPageId = result.newPage().getPageId();

        assertAll(
                () -> assertEquals(left, result.currentPage().getNumberOfDocuments()),
                () -> assertEquals(right, result.newPage().getNumberOfDocuments()),
                () -> assertEquals(99L, result.newPage().getPrevious()),
                () -> assertEquals(newPageId, result.currentPage().getNext())
        );

        System.out.println("Current Page: " + result.currentPage().getPageSize());
        System.out.println("New Page: " + result.newPage().getPageSize());
    }
}