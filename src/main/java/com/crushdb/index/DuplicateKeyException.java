package com.crushdb.index;

public class DuplicateKeyException extends RuntimeException {
    public DuplicateKeyException(String message) {
        super(message);
    }
}
