package com.crushdb.server.router;

import com.crushdb.server.http.RequestMethod;

import java.util.Objects;

public record RouteKey(RequestMethod requestMethod, String path) {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RouteKey other = (RouteKey) o;
        return requestMethod == other.requestMethod && Objects.equals(path, other.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestMethod, path);
    }
}
