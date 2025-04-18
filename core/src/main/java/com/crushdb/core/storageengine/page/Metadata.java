package com.crushdb.core.storageengine.page;

public record Metadata(int magicNumber, int version, long lastPageId) {
}
