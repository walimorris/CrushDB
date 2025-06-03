package com.crushdb.core.authentication;

public interface Authenticator {
    boolean authenticate(String username, String password);
    boolean isAuthenticated();
    User getCurrentUser();
    String hash(String value);
    boolean validatePassword(String plainPassword, String hashedPassword);
}
