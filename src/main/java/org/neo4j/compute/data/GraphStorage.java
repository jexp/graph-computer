package org.neo4j.compute.data;

import org.neo4j.graphdb.Direction;

import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * @author mh
 * @since 07.12.13
 */
public abstract class GraphStorage {
    private long totalRels;
    protected int totalNodes; // todo Long and several storage blocks
    protected int reallocation;

    public void init(long totalNodes, long totalRels) {
        this.totalNodes = (int) totalNodes;
        this.totalRels = totalRels;
    }

    public long getTotalRels() {
        return totalRels;
    }

    public int getTotalNodes() {
        return totalNodes;
    }

    protected int initial() {
        return initial(totalNodes,totalRels);
    }

    private int initial(int totalNodes, long totalRels) {
        return (int) Math.max(4, Math.pow(2, 1 + Math.ceil(Math.log(totalRels / totalNodes) / Math.log(2))));
    }

    public abstract long determineSize();

    public static long determineSize(Object data) {
        try {
            CountingOutputStream counter = new CountingOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(counter);
            os.writeObject(data);
            os.close();
            return counter.getCount();
        } catch (IOException ioe) {
            return -1;
        }
    }

    // type << 1 | dir -> int 0...x -> offset : direct jump to header (read 2 values: offset, next-offset) -> jump to offset and iterate
    // header of x bytes
    // alternative: mapping of type + dir -> header-offset -> subset of type, dir possible, mapping with header approach from above
    // per type and dir, target node id as delta, x(n) + x(n-1)
    // storage as VQT?? + RLL
    // or compressed bitset-chunks RLL encoded
    // int[][] -> byte[][] -> preallocated avg sized and aligned byte arrays
    // reallocSortEncode() -> first int -> size of encoded block, after that unsorted but encoded when limit is reached -> decode sort encode compress, reallocate block
    // todo choose if encode/compress according to available memory, i.e. we need rels * 2 * 4 bytes for uncompressed storage
    public abstract void addTarget(long nodeId, long target, int type, Direction direction);

    public int getReallocation() {
        return reallocation;
    }
}
