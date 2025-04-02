package com.crushdb.index;

import com.crushdb.index.btree.PageOffsetReference;
import com.crushdb.model.BsonValue;
import com.crushdb.model.Document;

import java.util.Optional;

/**
 * A builder class for creating instances of {@code IndexEntry} from a {@code Document}.
 */
public class IndexEntryBuilder {

    /**
     * Builds an {@link Optional} containing an {@link IndexEntry} from the specified {@link Document}.
     *
     * @param <T> the type of the key, which must extend {@link Comparable}
     * @param doc the document containing the data to extract
     * @param index the index this entry belongs
     *
     * @return an {@link Optional} containing the {@link IndexEntry} if the field exists and has a valid value,
     *         or an empty {@link Optional} if the field is not present or the value cannot be cast to the expected type
     */
    public static <T extends Comparable<T>> IndexEntry<T> fromDocument(Document doc, BPTreeIndex<T> index) {
        BsonValue keyVal = doc.get(index.getFieldName());
        if (keyVal == null) {
            return null;
        }
        try {
            @SuppressWarnings("unchecked")
            T key = (T) keyVal.toJavaValue();

            // the index can pull the page id and offset from the document and reference this when document is read
            return new IndexEntry<>(key, new PageOffsetReference(doc.getPageId(), doc.getOffset()));
        } catch (ClassCastException e) {
            return null;
        }
    }
}
