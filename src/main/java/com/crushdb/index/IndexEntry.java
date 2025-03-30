package com.crushdb.index;

import com.crushdb.index.btree.PageOffsetReference;

/**
 * Represents a single entry in a database index, associating a key with the location in page.
 *
 * @param <T> the type of the key, which must be {@link Comparable}
 */
public record IndexEntry<T extends Comparable<T>>(T key, PageOffsetReference reference) {}
