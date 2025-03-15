package com.crushdb.index;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BPMappingTest {

    @Test
    public void compareToTest() {
        PageOffsetReference pageOffsetReference1 = new PageOffsetReference(10L, 34);
        BPMapping<Long> bpMappingLong1 = new BPMapping<>(1L, pageOffsetReference1);

        PageOffsetReference pageOffsetReference2 = new PageOffsetReference(20L, 35);
        BPMapping<Long> bpMappingLong2 = new BPMapping<>(2L, pageOffsetReference2);

        PageOffsetReference pageOffsetReference3 = new PageOffsetReference(30L, 36);
        BPMapping<Long> bpMappingLong3 = new BPMapping<>(2L, pageOffsetReference3);

        assertEquals(-1, bpMappingLong1.compareTo(bpMappingLong2));
        assertEquals(1, bpMappingLong2.compareTo(bpMappingLong1));
        assertEquals(0, bpMappingLong3.compareTo(bpMappingLong2));
    }
}