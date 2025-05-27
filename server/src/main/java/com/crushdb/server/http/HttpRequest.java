package com.crushdb.server.http;

import java.io.InputStream;
import java.util.Optional;

import com.crushdb.server.MicroWebServer;
import com.crushdb.server.handler.RouteHandler;

/**
 * An HTTP request.
 *
 * <p>An {@code HttpRequest} instance is built through an {@code HttpRequest}
 * {@linkplain HttpRequest.Builder builder}. An {@code HttpRequest} builder
 * is obtained from one of the {@link HttpRequest#newBuilder(String) newBuilder}
 * methods. A request's path, {@link RequestMethod}, {@link HttpVersion},
 * {@link HttpHeaders}, {@link MimeType}, and body can be set. This version of
 * HttpRequest is internal to the usage of {@code CrushDB} {@link MicroWebServer}.
 * In general, this server is talking to a local browser which is requesting
 * resources from the underlying {@code CrushDB}, these requests are being
 * intercepted at the socket level, reading and building a response to pass back
 * to the browser. These are the requirements to fully speak with
 * your local Browser: A path to the requested resource, headers, mimetype, and
 * the stream from the browser. The {@code the default http version: HTTP/1.1}.
 * The {@code default method: GET}. Once all the required parameters have been
 * set in the builder, {@link Builder#build() build} will return the
 * {@link HttpRequest}. Builders can be copied and modified many times in order
 * to build multiple related requests that differ in some parameters.
 * {@link HttpRequest#twin()}
 *
 * <pre>{@code HttpRequest request = HttpRequest.newBuilder("/web/index.html").build();
 * HttpRequest requestTwin = request.twin();}</pre>
 *
 * <p>You now effectively have two {@linkplain HttpRequest httpRequests} on the same path
 * but potentially different parameters.
 *
 * <p>Internally, {@code MicroServer} listens on a socket and parses the raw HTTP request
 * into a {@linkplain HttpRequest httpRequest}. This consumed request provides structure
 * and adds {@code method, path, version, headers, stream}.</p>
 *
 * <p>The following is a complete example of this usage:
 *
 * <pre>{@code HttpRequest httpRequest = HttpRequest.newBuilder(path)
 *   .method(RequestMethod.GET)
 *   .version(HttpVersion.HTTP_1_1)
 *   .mimetype(MimeType.JS)
 *   .inputStream(stream)
 *   .build(); }</pre>
 *
 * @author walimorris
 */
public abstract class HttpRequest {

    /**
     * Creates an HttpRequest.
     */
    protected HttpRequest() {}

    /**
     * A builder of {@linkplain HttpRequest HTTP requests}.
     *
     * <p>Instances of {@code HttpRequest.Builder} are created by calling
     * {@link HttpRequest#newBuilder()} or {@link HttpRequest#newBuilder(String)}.
     *
     * <p> This builder currently supports configuring local requests. However,
     * this will expand in the future to communicate with external servers.
     * Internally, this builder consumes request-state, such as: the request path,
     * the request method {@code default: GET} unless explicitly set, headers,
     * etc. Each setter method modifies the state of the builder and returns the
     * instance. The {@link #build() build} method returns a new
     * {@link HttpRequest} each time it is invoked.
     */
    public interface Builder {

        /**
         * Set's this {@code HttpRequest}'s request {@code path}.
         *
         * @param path {@link String} requested resource
         *
         * @return this builder
         */
        Builder path(String path);

        /**
         * Internal {@code MicroServer} usage:
         *
         * <p>Browsers send requests that take this form:
         *
         * <pre>
         * GET / HTTP/1.1
         * Host: localhost:8080
         * </pre>
         *
         * {@code MicroServer} will handle parsing requests locally, however
         * the {@code GET} method is what can be morphed into a
         * {@code RequestMethod}.
         *
         * <p>Options:
         * <ul>
         *     <li>{@link RequestMethod#GET}</li>
         *     <li>{@link RequestMethod#POST}</li>
         *     <li>{@link RequestMethod#DELETE}</li>
         *     <li>{@link RequestMethod#DELETE}</li>
         * </ul>
         *
         * @param method {@link RequestMethod}
         *
         * @return this builder
         */
        Builder method(RequestMethod method);

        /**
         * Sets the {@link HttpVersion} for this request.
         * <p>Here's an example request from the browser:
         *
         * <pre>
         * GET /api/documents HTTP/ 1.1
         * Host: localhost:8080
         * </pre>
         *
         * {@code default: HTTP/ 1.1 unless explicitly set}.
         *
         * <p>Options:
         * <ul>
         *     <li>{@link HttpVersion#HTTP_1_1}</li>
         *     <li>{@link HttpVersion#HTTP_2}</li>
         * </ul>
         *
         * @param version {@link HttpVersion} http version
         *
         * @return this builder
         */
        Builder version(HttpVersion version);

        /**
         * Set's the headers for this request. Headers utilize
         * {@linkplain HttpHeader httpsHeader} objects. When
         * {@linkplain HttpRequest#headers() headers()} is
         * called, if headers are present on the request, they
         * are returned. However, for internal
         * {@code MicroServer} communication on the localhost,
         * headers can be built from a ready input stream. The
         * stream is analyzed for {@code MimeType} and minimal
         * headers are created with {@code Content-Type from
         * the MimeType, and Content-Length from the stream}.
         * {@link HttpHeader} needs to be a fully built key,
         * value pair, otherwise instantiation will fail. This
         * guarantees {@link HttpHeaders} with any value will
         * be complete.
         *
         * <p>Note: we are still building and adding support for
         * key(name), value pair. In the future, any invalid
         * headers will throw {@link IllegalArgumentException}
         * if it is not in compliance with
         * <a href="https://tools.ietf.org/html/rfc7230#section-3.2">
         *     RFC 7230 section-3.2</a>
         *
         * @param headers {@link HttpHeaders} request headers
         *
         * @return this builder.
         */
        Builder headers(HttpHeaders headers);

        /**
         * Set's the request {@linkplain MimeType mimeType}. The
         * {@code MimeType} assists with processing internal URLs
         * on {@code MicroServer}. A MimeType identifies the type
         * of resource being requested.
         *
         * @param mimeType {@link MimeType} request resource type
         *
         * @see MimeType
         * @return this builder
         */
        Builder mimetype(MimeType mimeType);

        /**
         * Set's the {@link InputStream inputstream} from the request from the
         * internal localhost. The input stream contains information needed to
         * configure the {@linkplain HttpRequest httpRequest}.
         *
         * @param inputStream {@link InputStream} request data
         *
         * @return this builder.
         */
        Builder inputStream(InputStream inputStream);

        /**
         * Builds a digital twin of this {@linkplain HttpRequest}.
         * This allows users to create identical httpRequests, but
         * modify them independently of this builder.
         *
         * @return this builder.
         */
        Builder twin();

        /**
         * Builds and returns an {@link HttpRequest}.
         *
         * <p>Must add failure, if path is not set. such as
         * throwing an IllegalState exception.
         *
         * @return a new {@linkplain HttpRequest httpRequest}
         */
        HttpRequest build();
    }

    /**
     * Creates an {@link HttpRequest} builder with the given path.
     *
     * @param path {@link String} resource path
     *
     * @return a new {@linkplain HttpRequest httpRequest} builder
     */
    public static HttpRequest.Builder newBuilder(String path) {
        return new HttpRequestBuilderImpl(path);
    }

    /**
     * Creates an {@link HttpRequest} builder.
     *
     * @return a new {@linkplain HttpRequest httpRequest} builder
     */
    public static HttpRequest.Builder newBuilder() {
        return new HttpRequestBuilderImpl();
    }

    /**
     * Return the {@linkplain HttpRequest httpRequest} requested
     * resource path.
     *
     * @return {@link String} path to requested resource
     */
    public abstract String path();

    /**
     * Return the {@linkplain HttpRequest httpRequest} method.
     * If not set the default method is {@link RequestMethod#GET}.
     *
     * @return {@link RequestMethod}
     */
    public abstract RequestMethod method();

    /**
     * Return an {@link Optional} {@linkplain HttpVersion httpVersion}
     * pulled from the given request to {@code MicroServer}. If not
     * set the default version is {@link HttpVersion#HTTP_1_1}.
     *
     * @return {@link Optional} containing this request's
     * {@link HttpVersion}
     */
    public abstract Optional<HttpVersion> version();

    /**
     * Return this request's {@linkplain HttpHeaders httpHeaders}. Headers
     * are <a href="https://tools.ietf.org/html/rfc7230#section-3.2">
     * RFC 7230 section-3.2</a> complaint.
     *
     * @see HttpHeader
     * @return {@link HttpHeaders}
     */
    public abstract HttpHeaders headers();

    /**
     * Return this request {@linkplain MimeType mimeType} which is traced from
     * the original request for resources.
     *
     * @return {@link MimeType}
     */
    public abstract MimeType mimeType();

    /**
     * Return the origin request {@linkplain InputStream inputStream}. The
     * input stream assists with configuring other properties for this
     * request, such as headers and mimeType. The input stream is also
     * used with {@linkplain RouteHandler routeHandler} and
     * {@linkplain HttpResponse httpResponse}.
     *
     * @return {@link InputStream}
     */
    public abstract InputStream inputStream();

    /**
     * Return digital twin of this request.
     *
     * @return {@link HttpRequest}
     */
    public abstract HttpRequest twin();
}
