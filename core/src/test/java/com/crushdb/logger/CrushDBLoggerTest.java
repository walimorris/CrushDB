package com.crushdb.logger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertAll;

public class CrushDBLoggerTest {

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