package com.crushdb.server.http;

public enum HttpVersion {

    /**
     * HTTP version 1.1
     */
    HTTP_1_1("HTTP/1.1"),

    /**
     * HTTP version 2
     */
    HTTP_2("HTTP/2");

    private final String version;

    HttpVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return this.version;
    }

    public static HttpVersion version(String version) {
        for (HttpVersion v : values()) {
            if (v.getVersion().equals(version)) {
                return v;
            }
        }
        return null;
    }
}
