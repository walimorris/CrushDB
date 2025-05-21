package com.crushdb.index;

import com.crushdb.core.bootstrap.CrushContext;
import com.crushdb.core.bootstrap.DatabaseInitializer;
import com.crushdb.core.index.btree.BPMapping;
import com.crushdb.core.index.btree.PageOffsetReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BPMappingTest {
    private static CrushContext cxt;

    @BeforeAll
    public static void setUp() {
        cxt = DatabaseInitializer.init();
    }

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

    @Test
    public void addReference() {
        String key = "Redman";
        PageOffsetReference pageOffsetReference1 = new PageOffsetReference(10L, 123);
        PageOffsetReference pageOffsetReference2 = new PageOffsetReference(20L, 223);
        PageOffsetReference pageOffsetReference3 = new PageOffsetReference(30L, 333);
        PageOffsetReference pageOffsetReference4 = new PageOffsetReference(49L, 444);

        BPMapping<String> bpMapping = new BPMapping<>(key, pageOffsetReference1);
        bpMapping.addReference(pageOffsetReference2);
        bpMapping.addReference(pageOffsetReference3);
        bpMapping.addReference(pageOffsetReference4);

        List<PageOffsetReference> pageOffsetReferences = bpMapping.getReferences();
        assertAll(
                () -> assertEquals(4, pageOffsetReferences.size()),
                () -> assertEquals(key, bpMapping.getKey())
        );
    }
}