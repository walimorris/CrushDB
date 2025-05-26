package com.crushdb.server.http;

/**
 * The representation data associated with an HTTP message is either provided as the
 * content of the message or referred to by the message semantics and the target URI.
 * The representation data is in a format and encoding defined by the representation
 * metadata header fields. If you would like to read more about headers and the valid
 * representation of these fields see:
 * <a href="https://datatracker.ietf.org/doc/html/rfc9110#name-representation-data-and-met">
 *     Representation Data and Metadata</a>
 *
 * <p>These header names represent the options for HTTP request and response headers
 * and provide the means of passing further metadata.
 *
 * @author walimorris
 */
public enum HeaderName {
    CONTENT_TYPE("Content-Type"),
    CONTENT_LENGTH("Content-Length"),
    X_CONTENT_TYPE_OPTIONS("X-Content-Type-Options");

    private final String headerName;

    HeaderName(String headerName) {
        this.headerName = headerName;
    }

    public String getHeaderName() {
        return this.headerName;
    }
}
