package com.crushdb.server.router;

import com.crushdb.server.http.RequestMethod;

import java.util.concurrent.ConcurrentHashMap;
import com.crushdb.server.config.RouterConfig;

/**
 * {@code com.crushdb.server.router.Router} works with the {@linkplain RouterConfig}
 * to register, resolve, execute and dispatch incoming HTTP requests to the appropriate
 * handler. This is generally done by mapping the incoming path and request method to
 * the handler.
 *
 * <p>We will introduce {@code Middleware} such as {@code LoggingMiddleWare},
 * {@code SecurityMiddleware} and {@code TransformationMiddleware} in order to
 * reduce complexity and abstract middleware logic from the {@code Router}.
 *
 * @see RouterConfig
 * @see RouteHandler
 *
 * @author walimorris
 */
public class Router {

    /**
     * Stores the {@link RouteKey} and {@link RouteHandler} pairs utilized
     * in dispatching incoming HTTP requests.
     */
    private final ConcurrentHashMap<RouteKey, RouteHandler> routes = new ConcurrentHashMap<>();

    /**
     * Registers {@link RouteKey} and {@link RouteHandler} pairs.
     *
     * @param routeKey {@link RouteKey} the request method and resource path
     * @param routeHandler {@link RouteHandler} handler that executes based on the route key
     */
    public void register(RouteKey routeKey, RouteHandler routeHandler) {
        routes.put(routeKey, routeHandler);
    }

    /**
     * Registers {@link RouteKey} and {@link RouteHandler} pairs given the {@link RequestMethod}
     * resource path and routeHandler.
     *
     * @param requestMethod {@link RequestMethod} http request method
     * @param path {@link String} resource path
     * @param routeHandler {@link RouteHandler} handler that executes based on the route key
     */
    public void register(RequestMethod requestMethod, String path, RouteHandler routeHandler) {
        register(new RouteKey(requestMethod, path), routeHandler);
    }

    /**
     * Resolves a route given the {@link RequestMethod} and {@link String} path.
     * Given the {@code RequestMethod} and {@code string Path}, a
     * {@link RouteKey} can be constructed and searched.
     *
     * <p>Searches {@code "/"} path if empty.
     *
     * @param requestMethod {@link RequestMethod} http request method
     * @param path {@link String} resource path
     *
     * @return {@link RouteHandler} that executes based on the route key
     */
    public RouteHandler resolve(RequestMethod requestMethod, String path) {
        if (path.isEmpty()) {
            path = "/";
        }
        return routes.get(new RouteKey(requestMethod, path));
    }

    /**
     * Resolves a "common route", in this case meaning a commonly used root. However,
     * the common route must be formatted in such a manner with the root beginning
     * with a forward slash. This functionality allows common paths, paths that are
     * used many times to resolve and handle those common routes pointing to closely
     * related resources: such as {@code "/web/*"} to get any web resource under
     * that common route.
     *
     * @param requestMethod {@link RequestMethod} http request method
     * @param path {@link String} resource path
     *
     * @return {@link RouteHandler} that executes based on the route key
     */
    public RouteHandler resolveCommonRoute(RequestMethod requestMethod, String path) {
        if (routes.isEmpty() || path.equals("/")) {
            return resolve(requestMethod, path);
        }
        if (path.startsWith("/")) {
            // '/web/static/main.js -> web/static/main.js' or /web -> web or /web/ -> web/, /web/static/ -> web/static/
            path = path.substring(1);
            if (!path.contains("/")) { // handle single part (web -> return immediately)
                return resolve(requestMethod, "/" + path + "/*");
            }
            if (path.endsWith("/")) {
                // web/static/ -> web/static
                path = path.substring(0, path.length() - 1);
            }
        }
        // web/static/main.js -> [web, static, main.js], web/static -> [web, static]
        return resolve(requestMethod, "/" + path.split("/")[0] + "/*");
    }
}
