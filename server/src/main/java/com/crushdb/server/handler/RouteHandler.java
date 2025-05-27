package com.crushdb.server.handler;


import com.crushdb.server.http.HttpRequest;
import com.crushdb.server.http.HttpResponse;

/**
 * A contract: defines the unit of logic responsible for generating a response
 * to a specific {@link HttpRequest httpRequest}. When {@code MicroServer}
 * receives a request to some path, a route handler decides what to do and
 * how to response.
 *
 * <p>Why a function interface makes this expressive:
 *
 * <p>Typical usage:
 * <pre>
 * {@code
 * ...
 * String path = "web/index.html";
 * RouteHandler handler = router.resolveCommonRoute(RequestMethod.GET, path);
 * HttpRequest request = HttpRequest.newBuilder(path);
 *   .method(RequestMethod.GET)
 *   .version(HttpVersion.HTTP_1_1)
 *   .mimeType(MimeType.HTML)
 *   .inputStream(stream)
 *   .build();
 * HttpResponse response = new HttpResponse();
 * handler.handle(request, response);
 * write(response, stream);}
 * </pre>
 *
 * <p>Functional usage:
 * <pre>
 * {@code
 * ...
 * router.resolve(RequestMethod.GET, "/ping", (request, response) -> {
 *     response.statusCode(200);
 *     response.body("received ping".getBytes());
 * });}
 * </pre>
 *
 * @author walimorris
 */
@FunctionalInterface
public interface RouteHandler {

    /**
     * Executes handler logic to manipulate the request and response objects.
     * Processes such as setting the status, setting the headers, and body.
     *
     * @param request {@link HttpRequest}
     * @param response {@link HttpResponse}
     */
    void handle(HttpRequest request, HttpResponse response);
}
