package org.neo4j.kernel.impl.nioneo.store.windowpool;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.util.StringLogger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author mh
 * @since 07.12.13
 */
public class DirectFileWindowPoolFactory implements WindowPoolFactory {
    static final int WINDOW_SIZE = 1024*1024;
    @Override
    public WindowPool create(File storageFileName, int recordSize, FileChannel fileChannel, Config configuration, StringLogger log) {
        return new DirectWindowPool(storageFileName, recordSize);
    }

    static class DirectWindowPool implements WindowPool, PersistenceWindow {

        private final File file;
        private final int recordSize;
        private final byte[] bytes;
        private long windowPosition;
        private Buffer buffer;
        private final BufferedInputStream is;
        private int bufferSize;
        private long fileOffset;

        public DirectWindowPool(File file, int recordSize) {
            this.file = file;
            this.fileOffset = 0;
            this.recordSize = recordSize;
            bytes = new byte[WINDOW_SIZE *recordSize];
            buffer = new Buffer(this, ByteBuffer.wrap(bytes));
            is = createInputStream(file);
            readNextBuffer();
        }

        private void readNextBuffer() {
            try {
                fileOffset += bufferSize;
                bufferSize = is.read(bytes);
            } catch (IOException e) {
                throw new RuntimeException("Error reading buffer",e);
            }
        }

        private BufferedInputStream createInputStream(File file) {
            try {
                return new BufferedInputStream(new FileInputStream(file), WINDOW_SIZE);
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Error opening file "+file,e);
            }
        }

        @Override
        public PersistenceWindow acquire(long position, OperationType operationType) {
            while (position - filePosition() >= WINDOW_SIZE ) {
                readNextBuffer();
            }
            this.windowPosition = position;
            if (operationType != OperationType.READ) throw new IllegalStateException("Read only");
            return this;
        }

        private long filePosition() {
            return fileOffset/recordSize;
        }

        @Override
        public void release(PersistenceWindow window) {
        }

        @Override
        public void flushAll() {
        }

        @Override
        public Buffer getBuffer() {
            return buffer;
        }

        @Override
        public Buffer getOffsettedBuffer(long id) {
            return getBuffer().setOffset((int) (id * recordSize - fileOffset)); // TODO
        }

        @Override
        public int getRecordSize() {
            return recordSize;
        }

        @Override
        public long position() {
            return windowPosition;
        }

        @Override
        public int size() {
            return bufferSize;
        }

        @Override
        public void force() {
        }

        @Override
        public void close() {
            try {
                is.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public WindowPoolStats getStats() {
            return null;
        }
    }
}
