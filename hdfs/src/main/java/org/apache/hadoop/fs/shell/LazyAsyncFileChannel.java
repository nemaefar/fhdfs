package org.apache.hadoop.fs.shell;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

public class LazyAsyncFileChannel implements Closeable {
    private final Path file;
    private final OpenOption[] options;
    private volatile AsynchronousFileChannel fileChannel;

    public LazyAsyncFileChannel(Path file, OpenOption... options) {
        this.file = file;
        this.options = options;
    }

    public AsynchronousFileChannel get() throws IOException {
        AsynchronousFileChannel result = fileChannel;

        if (result == null) {
            synchronized (this) {
                result = fileChannel;
                if (result == null) {
                    fileChannel = result = initialize();
                }
            }
        }

        return result;
    }

    private AsynchronousFileChannel initialize() throws IOException {
        return AsynchronousFileChannel.open(file, options);
    }

    @Override
    public void close() throws IOException {
        AsynchronousFileChannel result = fileChannel;

        if (result != null) {
            synchronized (this) {
                result = fileChannel;
                if (result != null) {
                    result.close();
                    fileChannel = null;
                }
            }
        }
    }
}
