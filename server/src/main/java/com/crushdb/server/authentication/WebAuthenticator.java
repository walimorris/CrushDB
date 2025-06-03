package com.crushdb.server.authentication;

import com.crushdb.core.authentication.Authenticator;
import com.crushdb.core.authentication.User;
import org.mindrot.jbcrypt.BCrypt;

import java.util.HashMap;

public class WebAuthenticator implements Authenticator {
    private User currentUser;
    private final HashMap<String, String> userStore = new HashMap<>();

    public WebAuthenticator() {
        // load db users from db sys file
        userStore.put("admin", hash("password123"));
    }

    @Override
    public boolean authenticate(String username, String password) {
        String hashed = userStore.get(username);
        if (hashed != null && validatePassword(password, hashed)) {
            this.currentUser = new User(username, hashed);
            return true;
        }
        return false;
    }

    @Override
    public boolean isAuthenticated() {
        return this.currentUser != null;
    }

    @Override
    public User getCurrentUser() {
        return this.currentUser;
    }

    @Override
    public String hash(String v) {
        return BCrypt.hashpw(v, BCrypt.gensalt());
    }

    @Override
    public boolean validatePassword(String plainPassword, String hashedPassword) {
        return BCrypt.checkpw(plainPassword, hashedPassword);
    }
}
