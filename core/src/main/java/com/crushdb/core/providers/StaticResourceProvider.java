package com.crushdb.core.providers;

import java.io.InputStream;

public interface StaticResourceProvider {
    InputStream getResource(String path);
}
