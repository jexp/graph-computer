package org.neo4j.compute.data;

import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.idcompression.bytes.SignedLongBase128EncoderBytes;
import org.neo4j.helpers.idcompression.bytes.UnsignedLongBase128EncoderBytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
* @author mh
* @since 07.12.13
*/
public class CompressedByteArrayGraphStorage extends GraphStorage {
    public static final int HEADER = 4;
    public static final int MAX_NUMBER_SIZE = 9;
    private byte[][] nodes;
    private final UnsignedLongBase128EncoderBytes unsignedEncoder = new UnsignedLongBase128EncoderBytes();
    private final SignedLongBase128EncoderBytes signedEncoder = new SignedLongBase128EncoderBytes();
    private long totalWritten = 0;

    public CompressedByteArrayGraphStorage() {
    }

    @Override
    public void addTarget(long nodeId, long target, int type, Direction direction) {
        int arrayOffset = (int) nodeId;
        byte[] node = nodes[arrayOffset];
        int entryOffset = node[0] + HEADER;
        if (entryOffset + MAX_NUMBER_SIZE >= node.length) {
            nodes[arrayOffset] = node = Arrays.copyOf(node, node.length * 2);
            reallocation ++;
        }
        int written = writeCompressed(node, entryOffset, target);// combine with type and direction
        totalWritten += written;
        writeInt(node, 0, entryOffset + written);
    }

    private int writeCompressed(byte[] bytes, int offset, long value) {
        return signedEncoder.encode(bytes,offset,value);
    }
    private int writeCompressed(byte[] bytes, int offset, int value) {
        return signedEncoder.encode(bytes,offset,value);
    }

    private int writeCompressed(byte[] bytes, int offset, byte value) {
        return signedEncoder.encode(bytes,offset,value);
    }

    private int writeInt(byte[] bytes, int offset, int value) {
        return writeCompressed(bytes,offset,value);
    }

    @Override
    public void init(long totalNodes, long totalRels) {
        super.init(totalNodes, totalRels);
        int initialBlockInBytes = initial() * 4 + HEADER + MAX_NUMBER_SIZE + 3;
        System.err.printf("Initial nodes %d rels %d rels/node %d block in bytes %d%n", totalNodes, totalRels, totalRels / totalNodes, initialBlockInBytes);
        this.nodes = new byte[this.totalNodes][initialBlockInBytes];
    }

    public void close() {

    }

    @Override
    public long determineSize() {
        long sum = 0;
        for (byte[] node : nodes) {
            sum += node.length;
        }
        return sum;
    }
}
