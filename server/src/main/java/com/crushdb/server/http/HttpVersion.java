package com.crushdb.server.http;

/**
 * {@code com.crushdb.server.http.HttpVersion} establishes the protocol version
 * used for {@linkplain HttpRequest httpRequests}
 * and {@linkplain HttpResponse httpResponses}. Http versions establish
 * a set of standardizations for HTTP.
 * <p>
 * <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Evolution_of_HTTP">
 *     HTTP and versioning</a>
 *
 * @author walimorris
 */
public enum HttpVersion {

    /**
     * HTTP version 1.1
     */
    HTTP_1_1("HTTP/1.1"),

    /**
     * HTTP version 2
     */
    HTTP_2("HTTP/2");

    /**
     * This HTTP version string.
     */
    private final String version;

    /**
     * Constructor that passes the HTTP version
     * as a string.
     *
     * @param version {@link String} HTTP version
     */
    HttpVersion(String version) {
        this.version = version;
    }

    /**
     * Return this HTTP version as a string.
     *
     * @return {@link String} HTTP version
     */
    public String getVersion() {
        return this.version;
    }

    /**
     * Return this HTTP version string representation as
     * a {@linkplain HttpVersion httpVersion} ENUM.
     *
     * @param version HTTP version string representation
     *
     * @return {@link HttpVersion} ENUM
     */
    public static HttpVersion version(String version) {
        for (HttpVersion v : values()) {
            if (v.getVersion().equals(version)) {
                return v;
            }
        }
        return null;
    }
}
