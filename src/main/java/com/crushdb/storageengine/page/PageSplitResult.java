package com.crushdb.storageengine.page;

public class PageSplitResult {
    private final Page currentPage;
    private final Page newPage;

    public PageSplitResult(Page currentPage, Page newPage) {
        this.currentPage = currentPage;
        this.newPage = newPage;
    }

    public Page getCurrentPage() { return currentPage; }
    public Page getNewPage() { return newPage; }
}
