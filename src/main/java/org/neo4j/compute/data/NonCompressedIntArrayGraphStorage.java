package org.neo4j.compute.data;

import org.neo4j.graphdb.Direction;

import java.util.Arrays;

/**
* @author mh
* @since 07.12.13
*/
public class NonCompressedIntArrayGraphStorage extends GraphStorage {
    private int[][] nodes;

    @Override
    public void addTarget(long nodeId, long target, int type, Direction direction) {
        int arrayOffset = (int) nodeId;
        int[] node = nodes[arrayOffset];
        int entryOffset = node[0] + 1;
        if (entryOffset == node.length) {
            nodes[arrayOffset] = node = Arrays.copyOf(node, node.length * 2);
            reallocation ++;
        }
        node[0] = entryOffset;
        node[entryOffset] = (int) target; // combine with type and direction
    }

    @Override
    public void init(long totalNodes, long totalRels) {
        super.init(totalNodes, totalRels);
        this.nodes = new int[this.totalNodes][initial()];
    }

    @Override
    public long determineSize() {
        return determineSize(nodes);
    }
}
