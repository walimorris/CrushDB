package com.crushdb.server.router;

import com.crushdb.server.http.RequestMethod;

public record RouteKey(RequestMethod requestMethod, String path) { }
