package org.apache.hadoop.fs.shell;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.stream.Stream;

public class EvictingQueue<E> {

    private final Queue<E> delegate;
    private final int maxSize;

    private EvictingQueue(int maxSize) {
        this.delegate = new ArrayDeque<E>(maxSize);
        this.maxSize = maxSize;
    }

    public static <E> EvictingQueue<E> create(int maxSize) {
        return new EvictingQueue<E>(maxSize);
    }

    Stream<E> stream() {
        return delegate.stream();
    }

    public boolean add(E e) {
        if (maxSize == 0) {
            return true;
        }
        if (size() == maxSize) {
            delegate.remove();
        }
        delegate.add(e);
        return true;
    }

    public int size() {
        return delegate.size();
    }
}
