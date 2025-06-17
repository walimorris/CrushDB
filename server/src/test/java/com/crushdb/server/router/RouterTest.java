package com.crushdb.server.router;

import com.crushdb.server.handler.AuthenticationHandler;
import com.crushdb.server.handler.PageHandler;
import com.crushdb.server.http.RequestMethod;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

class RouterTest {

    @Test
    void testRegisterAndResolve() {
        Router router = new Router();
        RouteKey homeRouteKey = new RouteKey(RequestMethod.GET, "/");
        RouteKey signinRouteKey = new RouteKey(RequestMethod.GET, "/web/signin.html");
        RouteKey wildcardRouteKey = new RouteKey(RequestMethod.GET, "/web/*");
        PageHandler pageHandler = Mockito.mock(PageHandler.class);
        AuthenticationHandler authenticationHandler = Mockito.mock(AuthenticationHandler.class);

        router.register(homeRouteKey, pageHandler);
        router.register(signinRouteKey, authenticationHandler);
        router.register(wildcardRouteKey, pageHandler);
        assertEquals(pageHandler, router.resolve(RequestMethod.GET, ""));
        assertEquals(pageHandler, router.resolve(RequestMethod.GET, "/"));
        assertEquals(pageHandler, router.resolve(RequestMethod.GET, "/web/*"));
        assertEquals(authenticationHandler, router.resolve(RequestMethod.GET, "/web/signin.html"));
    }

    @Test
    void testNullPathThrowsException() {
        Router router = new Router();
        assertThrows(NullPointerException.class, () -> router.resolve(RequestMethod.GET, null));
    }

    @Test
    void resolveCommonRoute() {
        Router router = new Router();
        PageHandler pageHandler = Mockito.mock(PageHandler.class);
        AuthenticationHandler authenticationHandler = Mockito.mock(AuthenticationHandler.class);
        RouteKey routeKey = new RouteKey(RequestMethod.GET, "/web/*");
        RouteKey authRouteKey = new RouteKey(RequestMethod.GET, "/web/signin.html");

        router.register(routeKey, pageHandler);
        router.register(authRouteKey, authenticationHandler);
        assertEquals(pageHandler, router.resolveCommonRoute(RequestMethod.GET, "/web/static/main.js"));
        assertEquals(authenticationHandler, router.resolveCommonRoute(RequestMethod.GET, "/web/signin.html"));
    }
}