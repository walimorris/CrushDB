package com.crushdb.server.handler;

import com.crushdb.core.bootstrap.CrushContext;
import com.crushdb.server.http.*;

/**
 * Contractor: handles general Page rendering requests to {@code MicroServer}.
 * The {@linkplain PageHandler} sets an {@link ImmutableHttpRequest}. State in
 * the {@code ImmutableHttpRequest} can not be manipulated or changed. Other
 * state such as status, headers, and version are also added to the response.
 *
 * @see ImmutableHttpRequest
 */
public class PageHandler implements RouteHandler {

    @Override
    public void handle(HttpRequest request, HttpResponse httpResponse, CrushContext cxt) {
        httpResponse.setImmutableHttpRequest((ImmutableHttpRequest) request);
        httpResponse.setHeaders(request.headers());
        httpResponse.setVersion(request.version());
        httpResponse.setStatusCode(200);
    }
}
