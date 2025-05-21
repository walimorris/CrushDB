package com.crushdb.core.bootstrap;

public class TestCrushContext extends CrushContext {

    @Override
    public boolean isTest() {
        return true;
    }
}
