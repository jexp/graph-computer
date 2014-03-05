package org.neo4j.compute.data;

import org.neo4j.graphdb.Direction;

/**
 * @author mh
 * @since 08.12.13
 */
public class NullGraphStorage extends GraphStorage {

    @Override
    public long determineSize() {
        return 0;
    }

    @Override
    public void addTarget(long nodeId, long target, int type, Direction direction) {
    }
}
