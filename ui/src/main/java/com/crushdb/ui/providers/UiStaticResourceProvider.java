package com.crushdb.ui.providers;

import com.crushdb.core.providers.StaticResourceProvider;

import java.io.InputStream;

public class UiStaticResourceProvider implements StaticResourceProvider {

    @Override
    public InputStream getResource(String path) {
        return getClass().getResourceAsStream(path);
    }
}
