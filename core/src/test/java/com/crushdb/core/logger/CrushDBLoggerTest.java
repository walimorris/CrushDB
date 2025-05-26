package com.crushdb.core.logger;

import com.crushdb.core.bootstrap.CrushContext;
import com.crushdb.core.bootstrap.DatabaseInitializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertAll;

public class CrushDBLoggerTest {
    private static CrushContext cxt;

    @BeforeAll
    public static void setUp() {
        cxt = DatabaseInitializer.init();
    }

    @Test
    public void initCrushDBLogger() {
        CrushDBLogger logger = CrushDBLogger.getLogger(CrushDBLoggerTest.class);
        assertAll(
                () -> assertEquals(10, logger.getMaxLogFiles()),
                () -> assertEquals(7, logger.getMaxLogRetentionDays()),
                () -> assertEquals(52428800, logger.getMaxLogSize())
        );
        logger.info("CrushDB Logger Initialized", null);
    }
}