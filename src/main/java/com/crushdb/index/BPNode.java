package com.crushdb.index;

public class BPNode <T extends Comparable<T>> {
    BPInternalNode<T> parent;

    public BPInternalNode<T> getParent() {
        return parent;
    }

    public void setParent(BPInternalNode<T> parent) {
        this.parent = parent;
    }
}
