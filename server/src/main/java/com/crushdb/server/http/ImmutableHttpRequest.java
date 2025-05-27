package com.crushdb.server.http;

import java.io.InputStream;
import java.util.Optional;
import com.crushdb.server.handler.RouteHandler;

import static java.util.Objects.requireNonNull;

/**
 * An {@code ImmutableHttpRequest} state cannot be changed once these values are set.
 * The {@link HttpResponse} that works with a companion {@link HttpRequest} will
 * contain this {@code ImmutableHttpRequest} representing the original httpRequest.
 * A response has no rights to changing the original state of the request, however
 * it is free to examine the state of the origin request. Further, the original
 * request state should not be tampered with for any auditing purpose. Calling
 * {@code twin()} will return a reference to this {@code ImmutableHttpRequest}.
 * The twin is only a deep copy and nothing more, mutations are still locked.
 *
 * @see HttpRequest
 * @see RouteHandler
 *
 * @author walimorris
 */
public class ImmutableHttpRequest extends HttpRequest {

    /**
     * This request's {@link RequestMethod}.
     */
    private final RequestMethod method;

    /**
     * This request's requested resource path.
     */
    private final String path;

    /**
     * This request's {@link HttpVersion}.
     */
    private final Optional<HttpVersion> version;

    /**
     * This request's {@link HttpHeaders}.
     */
    private final HttpHeaders headers;

    /**
     * This request's {@link MimeType}.
     */
    private final MimeType mimeType;

    /**
     * This request's input stream.
     */
    private final InputStream inputStream;

    /**
     * The {@code ImmutableHttpRequest} constructor. Utilizes the original
     * {@link HttpRequestBuilderImpl} builder to build this immutable
     * request's fields.
     *
     * @param builder {@link HttpRequestBuilderImpl} builder
     */
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
        return this;
    }
}
