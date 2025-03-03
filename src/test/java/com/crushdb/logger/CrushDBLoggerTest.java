package com.crushdb.logger;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CrushDBLoggerTest {

    @Test
    public void initDBLogger() {
        CrushDBLogger logger = CrushDBLogger.getLogger(CrushDBLoggerTest.class);
        assertAll(
                () -> assertEquals(10, logger.getMaxLogFiles()),
                () -> assertEquals(7, logger.getMaxLogRetentionDays()),
                () -> assertEquals(52428800, logger.getMaxLogSize())
        );
    }

}