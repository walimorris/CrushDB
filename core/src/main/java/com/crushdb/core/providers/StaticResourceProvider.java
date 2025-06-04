package com.crushdb.core.providers;

import java.io.InputStream;

/**
 * contract: {@code StaticResourceProvider} interface for Service Interface Provider
 * (SPI) that allows internal static resource sharing between {@code CrushDB} modules.
 * Modules that need authoritative access to this SPI should implement this class and
 * its methods.
 *
 * @author walimorris
 */
public interface StaticResourceProvider {

    /**
     * Get resource from given path.
     *
     * @param path {@link String} resource path
     *
     * @return {@link InputStream}
     */
    InputStream getResource(String path);
}
