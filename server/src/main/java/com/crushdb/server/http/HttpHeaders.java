package com.crushdb.server.http;

import java.util.Arrays;
import java.util.HashSet;

/**
 * {@code com.crushdb.server.http.HttpHeaders} are used on {@linkplain HttpRequest httpRequests}
 * and {@linkplain HttpResponse httpResponses} to pass requests metadata. {@code HttpHeaders}
 * are a single object that contain multiple {@linkplain HttpHeader httpHeader}.
 *
 * @see HttpHeader
 *
 * @author walimorris
 */
public class HttpHeaders {

    /**
     * Stores this {@linkplain HttpHeaders}. A {@link HttpHeader} is a
     * single instance of key (name), value pair. This instance of
     * HttpHeaders can store multiple httpHeader.
     */
    private final HashSet<HttpHeader> headersSet;

    /**
     * Set these {@linkplain HttpHeaders httpHeaders}.
     */
    public HttpHeaders() {
        headersSet = new HashSet<>();
    }

    /**
     * Add a single {@link HttpHeader} to this set of httpHeaders.
     *
     * @param header {@link HttpHeader} to add to these httpHeaders
     */
    public void addHeader(HttpHeader header) {
        headersSet.add(header);
    }

    /**
     * Adds a list of {@link HttpHeader} to this set of httpHeaders.
     *
     * @param headers list of {@link HttpHeader} to add to these httpHeaders
     */
    public void addHeaders(HttpHeader... headers) {
        headersSet.addAll(Arrays.asList(headers));
    }

    /**
     * Clears all {@linkplain HttpHeader httpHeader} in this httpHeaders.
     */
    public void clear() {
        headersSet.clear();
    }

    /**
     * Return the underlying {@link HashSet} of this {@code HttpHeaders}.
     *
     * @return {@link HashSet}
     */
    public HashSet<HttpHeader> set() {
        return headersSet;
    }

    /**
     * Get the current number of {@link HttpHeader} in these {@code HttpHeaders}.
     *
     * @return int number of httpHeaders
     */
    public int size() {
        return headersSet.size();
    }
}
