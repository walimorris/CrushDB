package com.crushdb.server.http;

import java.io.InputStream;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ImmutableHttpRequest extends HttpRequest {

    private final RequestMethod method;
    private final String path;
    private final Optional<HttpVersion> version;
    private final HttpHeaders headers;
    private final MimeType mimeType;
    private final InputStream inputStream;

    ImmutableHttpRequest(HttpRequestBuilderImpl builder) {
        this.method = requireNonNull(builder.method());
        this.path = requireNonNull(builder.path());
        this.version = requireNonNull(builder.version());
        this.headers = requireNonNull(builder.headers());
        this.mimeType = requireNonNull(builder.mimeType());
        this.inputStream = requireNonNull(builder.inputStream());
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public RequestMethod method() {
        return method;
    }

    @Override
    public Optional<HttpVersion> version() {
        return version;
    }

    @Override
    public HttpHeaders headers() {
        return headers;
    }

    @Override
    public MimeType mimeType() {
        return mimeType;
    }

    @Override
    public InputStream inputStream() {
        return inputStream;
    }

    @Override
    public HttpRequest twin() {
        return null;
    }
}
