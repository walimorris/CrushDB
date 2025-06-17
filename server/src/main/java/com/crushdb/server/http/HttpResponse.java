package com.crushdb.server.http;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class HttpResponse {
    private HttpHeaders headers;
    private int statusCode;
    private ImmutableHttpRequest immutableHttpRequest;
    private Optional<HttpVersion> version;

    private static final String RN = "\r\n";

    public HttpResponse() {}

    public HttpResponse(ImmutableHttpRequest immutableHttpRequest,
                        HttpHeaders headers,
                        int statusCode,
                        Optional<HttpVersion> version) {
        this.headers = headers;
        this.statusCode = statusCode;
        this.immutableHttpRequest = immutableHttpRequest;
        this.version = version;
    }

    public byte[] byteHeaders() {
        if (immutableHttpRequest.version().isEmpty()) {
            throw new IllegalStateException("no reference to version. Cannot respond.");
        }
        StringBuilder sb = new StringBuilder();
        for (HttpHeader header : headers.set()) {
            sb.append(header.getHeaderName())
                    .append(": ")
                    .append(header.getValue())
                    .append(RN);
        }
        sb.append(RN);
        String headers = immutableHttpRequest.version().get().getVersion() + " " +
               statusCode() + " OK" + RN + sb;
        return headers.getBytes(StandardCharsets.UTF_8);
    }

    public byte[] body() {
        try (InputStream inputStream = immutableHttpRequest.inputStream()) {
            return inputStream.readAllBytes();
        } catch (IOException e) {
            System.out.println("Error reading response body.");
        }
        return null;
    }

    public ImmutableHttpRequest immutableRequest() {
        return immutableHttpRequest;
    }

    public HttpHeaders headers() {
        return headers;
    }

    public int statusCode() {
        return statusCode;
    }

    public Optional<HttpVersion> version() {
        return version;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public void setHeaders(HttpHeaders headers) {
        this.headers = headers;
    }

    public void setImmutableHttpRequest(ImmutableHttpRequest immutableHttpRequest) {
        this.immutableHttpRequest = immutableHttpRequest;
    }

    public void setVersion(Optional<HttpVersion> version) {
        this.version = version;
    }
}
