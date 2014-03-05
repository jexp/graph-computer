package org.neo4j.compute.data;

import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.idcompression.LongEncoder;
import org.neo4j.helpers.idcompression.SignedLongBase128Encoder;
import org.neo4j.helpers.idcompression.SimpleLongEncoder;
import org.neo4j.helpers.idcompression.bytes.UnsignedLongBase128EncoderBytes;

import java.nio.ByteBuffer;

/**
 * @author mh
 * @since 07.12.13
 */
public class CompressedByteBufferGraphStorage extends GraphStorage {
    public static final int HEADER = 4;
    public static final int MAX_NUMBER_SIZE = 9;
    private byte[] nodes;
    private ByteBuffer buffer;
//    private final SignedLongBase128Encoder signedEncoder = new SignedLongBase128Encoder();
    private final LongEncoder signedEncoder = new SimpleLongEncoder();
    private long totalWritten = 0;
    private int blockSize;
    private int firstFreeBlock;

    public CompressedByteBufferGraphStorage() {
    }

    static class DebugInfo {
        int blockSize;
        long nodeId;
        int startArrayOffset; int startEntryOffset;
        int arrayOffset; int entryOffset;
        int newEntryOffset;
        int readPos;
        long delta;
        long lastValue;
        int writtenDelta, writtenHeader, writtenValue;
        int pages;
        int reallocArrayOffset;
        int writePos;

        @Override
        public String toString() {
            return "DebugInfo{" +
                    "blockSize=" + blockSize +
                    ", nodeId=" + nodeId +
                    ", startArrayOffset=" + startArrayOffset +
                    ", startEntryOffset=" + startEntryOffset +
                    ", arrayOffset=" + arrayOffset +
                    ", entryOffset=" + entryOffset +
                    ", newEntryOffset=" + newEntryOffset +
                    ", readPos=" + readPos +
                    ", delta=" + delta +
                    ", lastValue=" + lastValue +
                    ", writtenDelta=" + writtenDelta +
                    ", writtenHeader=" + writtenHeader +
                    ", writtenValue=" + writtenValue +
                    ", pages=" + pages +
                    ", reallocArrayOffset=" + reallocArrayOffset +
                    ", writePos =" + writePos +
                    '}';
        }

        public DebugInfo clean() {
            blockSize = -1; nodeId = -1; arrayOffset = -1; entryOffset = -1;startArrayOffset = -1; startEntryOffset = -1; newEntryOffset = -1;
            readPos = -1; delta = 0;
            lastValue = -1; writtenDelta = 0; writtenHeader = 0; writtenValue = 0;
            pages = 0;
            writePos = -1;
            reallocArrayOffset = -1;
            return this;
        }
    }
    // only compress if normal integer storage exceeds block?
    // at arrayOffset -> 0 < local offset  <  x * blockSize
    // local offset last written value
    // when local offset % blockSize == 0 full block -> arrayOffset -> next page offset
    DebugInfo debugInfo = new DebugInfo();

    @Override
    public void addTarget(long nodeId, long target, int type, Direction direction) {
        addTarget(nodeId, target, type, direction, debugInfo);
    }
    public void addTarget(long nodeId, long target, int type, Direction direction, DebugInfo d) {
        d.blockSize = blockSize; d.nodeId = nodeId;
        int arrayOffset = d.startArrayOffset = (int) nodeId * blockSize;
        int entryOffset = d.startEntryOffset = (int) readCompressed(buffer, arrayOffset);
        while (entryOffset > blockSize) { // skip to next page
            arrayOffset += entryOffset;
            entryOffset = (int) readCompressed(buffer, arrayOffset);
            d.pages ++;
        }
        d.arrayOffset = arrayOffset;
        d.entryOffset = entryOffset;

        int readPos = d.readPos = arrayOffset + entryOffset + HEADER;
        long lastValue = 0;
        if ( entryOffset > 0 ) {
            lastValue = d.lastValue = readCompressed(buffer, readPos );
        }
        long delta = d.delta =  target - lastValue;
        int writtenDelta = d.writtenDelta = writeCompressed(buffer, readPos, delta);
        int newEntryOffset = entryOffset + writtenDelta;
        int writtenValue = d.writtenValue = size(target);
        int writtenHeader = 0;
        if (newEntryOffset + writtenValue  + HEADER >= blockSize) {
            int newArrayOffset = d.reallocArrayOffset = nextFreeBlock();
            writtenHeader += writeCompressed(buffer,arrayOffset,newArrayOffset-arrayOffset);
            arrayOffset = newArrayOffset;
            newEntryOffset = 0;
            reallocation++;
        }
        d.newEntryOffset = newEntryOffset;
        writeCompressed(buffer, arrayOffset + newEntryOffset + HEADER, target);
        d.writePos = arrayOffset + newEntryOffset + HEADER;
        writtenHeader += writeCompressed(buffer, arrayOffset, newEntryOffset);
        d.writtenHeader = writtenHeader;
        totalWritten += writtenDelta + writtenValue + writtenHeader;
    }

    private int size(long target) {
        return signedEncoder.size(target);
    }

    private int nextFreeBlock() {
        // todo handle buffer overflow
        // todo handle differently sized blocks
        firstFreeBlock+=blockSize;
        return firstFreeBlock-blockSize;
    }

    private int writeCompressed(ByteBuffer buffer, int offset, long value) {
        try {
            buffer.position(offset);
            return signedEncoder.encode(buffer, value);
        } catch(Exception e) {
            throw e;
        }
    }

    private long readCompressed(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        return signedEncoder.decode(buffer);
    }

    @Override
    public void init(long totalNodes, long totalRels) {
        super.init(totalNodes, totalRels);
        blockSize = initial() * 4 + HEADER + MAX_NUMBER_SIZE + 3;
        int bufferSize = (int) (this.totalNodes * blockSize * 1.5);
        System.err.printf("Initial nodes %d rels %d rels/node %d block in bytes %d buffer size %d %n", totalNodes, totalRels, totalRels / totalNodes, blockSize,bufferSize);
        this.nodes = new byte[bufferSize];

        this.buffer = ByteBuffer.wrap(nodes);
    }

    public void close() {
        System.out.printf("size %d written %d%n",determineSize(),totalWritten);
    }

    @Override
    public long determineSize() {
        return nodes.length;
    }
}
