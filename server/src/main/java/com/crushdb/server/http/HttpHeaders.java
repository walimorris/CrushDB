package com.crushdb.server.http;

import java.util.Arrays;
import java.util.HashSet;

public class HttpHeaders {
    private final HashSet<HttpHeader> headersSet;

    public HttpHeaders() {
        headersSet = new HashSet<>();
    }

    public void addHeader(HttpHeader header) {
        headersSet.add(header);
    }

    public void addHeaders(HttpHeader... headers) {
        headersSet.addAll(Arrays.asList(headers));
    }

    public void clear() {
        headersSet.clear();
    }

    public HashSet<HttpHeader> set() {
        return headersSet;
    }

    public int size() {
        return headersSet.size();
    }
}
