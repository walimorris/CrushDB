package com.crushdb.server.http;

import com.crushdb.server.utils.StreamingUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HttpRequestBuilderImpl implements HttpRequest.Builder {

    private RequestMethod method;
    private String path;
    private volatile Optional<HttpVersion> version;
    private volatile HttpHeaders headers;
    private MimeType mimeType;
    private InputStream inputStream;

    public HttpRequestBuilderImpl(String path) {
        requireNonNull(path, "path must be non-null");
        checkPath(path);
        this.path = path;
        this.method = RequestMethod.GET; // default method
        this.version = Optional.of(HttpVersion.HTTP_1_1);
    }

    public HttpRequestBuilderImpl() {
        this.method = RequestMethod.GET;
        this.version = Optional.of(HttpVersion.HTTP_1_1);
    }

    static void checkPath(String path) {
        System.out.println("TODO: Run security checks on path!");
    }

    static boolean checkHeader(HttpHeader header) {
        System.out.println("TODO: Run security checks on header!");
        return true;
    }

    @Override
    public HttpRequestBuilderImpl path(String path) {
        requireNonNull(path, "path must not be null");
        this.path = path;
        return this;
    }

    @Override
    public HttpRequestBuilderImpl method(RequestMethod method) {
        requireNonNull(method, "request method must not be null.");
        this.method = method;
        return this;
    }

    @Override
    public HttpRequestBuilderImpl version(HttpVersion version) {
        requireNonNull(version);
        this.version = Optional.of(version);
        return this;
    }

    @Override
    public synchronized HttpRequest.Builder headers(HttpHeaders headers) {
        requireNonNull(headers);
        if (headers.size() == 0 || headers.size() % 2 != 0) {
            throw new IllegalArgumentException();
        }
        for (HttpHeader header : headers.set()) {
            if (checkHeader(header)) {
                this.headers.addHeader(header);
            }
        }
        return this;
    }

    @Override
    public HttpRequestBuilderImpl mimetype(MimeType mimeType) {
        requireNonNull(mimeType);
        this.mimeType = mimeType;
        return this;
    }

    @Override
    public HttpRequestBuilderImpl inputStream(InputStream inputStream) {
        requireNonNull(inputStream);
        this.inputStream = inputStream;
        return this;
    }

    @Override
    public synchronized HttpRequest.Builder twin() {
        HttpRequestBuilderImpl rb = new HttpRequestBuilderImpl();
        rb.path = this.path;
        rb.headers = this.headers;
        rb.method = this.method;
        rb.version = this.version;
        rb.mimeType = this.mimeType;
        rb.inputStream = this.inputStream;
        return rb;
    }

    @Override
    public HttpRequest build() {
        if (path == null) {
            throw new IllegalStateException("path is null");
        }
        assert method != null;
        return  new ImmutableHttpRequest(this);
    }

    String path() {
        return path;
    }

    RequestMethod method() {
        return method;
    }

    Optional<HttpVersion> version() {
        return version;
    }

    HttpHeaders headers() {
        if (this.headers != null) {
            return this.headers;
        }
        synchronized (this) {
            if (this.inputStream == null) {
                return null;
            } else {
                try {
                    if (this.mimeType == null) {
                        throw new IllegalStateException("mimetype cannot be null");
                    }
                    byte[] content = StreamingUtil.readMarkAndReset(this.inputStream);
                    HttpHeaders result = new HttpHeaders();
                    result.addHeader(new HttpHeader("Content-Type", mimeType().getType()));
                    result.addHeader(new HttpHeader("Content-Length", String.valueOf(content.length)));
                    this.headers = result;
                    return result;
                } catch (IOException e) {
                    System.out.println("Error parsing headers from input stream: " + e.getMessage());
                    return null;
                }
            }
        }
    }

    MimeType mimeType() {
        return mimeType;
    }

    InputStream inputStream() {
        return inputStream;
    }
}
