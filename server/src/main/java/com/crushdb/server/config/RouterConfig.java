package com.crushdb.server.config;

import com.crushdb.server.handler.PageHandler;
import com.crushdb.server.router.Router;
import com.crushdb.server.http.RequestMethod;
import com.crushdb.server.handler.RouteHandler;

import static com.crushdb.server.http.RequestMethod.GET;

/**
 * {@code com.crushdb.server.config.RouterConfig} is used to register
 * {@link RequestMethod} and resource path to its {@link RouteHandler}.
 *
 * @author walimorris
 */
public class RouterConfig {

    /**
     * Initialize Router.
     *
     * @return {@link Router}
     */
    public static Router init() {
        Router router = new Router();
        router.register(GET, "/web/index.html", new PageHandler());
        router.register(GET, "/web/*", new PageHandler());
        return router;
    }
}
