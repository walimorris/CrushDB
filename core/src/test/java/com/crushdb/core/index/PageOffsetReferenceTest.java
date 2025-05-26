package com.crushdb.core.index;

import com.crushdb.core.index.btree.PageOffsetReference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PageOffsetReferenceTest {

    @Test
    void getPageId() {
        PageOffsetReference pageOffsetReference = new PageOffsetReference(1L, 34);
        assertEquals(1L, pageOffsetReference.getPageId());
    }

    @Test
    void getOffset() {
        PageOffsetReference pageOffsetReference = new PageOffsetReference(2L, 45);
        assertEquals(45, pageOffsetReference.getOffset());
    }
}