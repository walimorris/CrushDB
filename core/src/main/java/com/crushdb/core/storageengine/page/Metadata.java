package com.crushdb.storageengine.page;

public record Metadata(int magicNumber, int version, long lastPageId) {
}
