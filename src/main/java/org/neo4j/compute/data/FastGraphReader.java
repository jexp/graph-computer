package org.neo4j.compute.data;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.windowpool.DirectNeoStore;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author mh
 * @since 29.11.13
 */
public class FastGraphReader {

    public static final int MB = 1024 * 1024;
    private final GraphStorage graphStorage =  new CompressedByteBufferGraphStorage();
    // new NullGraphStorage(); // new CompressedByteArrayGraphStorage(); // new NonCompressedIntArrayGraphStorage();
    private DirectNeoStore neoStore;

    public FastGraphReader() {
    }


    public void init(String storeDir) {
        neoStore = new DirectNeoStore(storeDir);
        System.out.printf("store %s nodes %dMB -> %d rels %dMB -> %d%n", storeDir, neoStore.getNodeStoreSize() / MB, neoStore.getTotalNodes(),
                neoStore.getRelStoreSize() / MB, neoStore.getTotalRels());
    }

    public void close() {
        neoStore.close();
    }

    public static void main(String[] args) throws IOException {
        FastGraphReader reader = null;
        try {
            reader = new FastGraphReader();
            reader.init(args[0]);
            long time=System.currentTimeMillis();
            GraphStorage storage = reader.read();
            System.out.printf("time %d s, size %d MB %d reallocs %n", (System.currentTimeMillis() - time) / 1000, storage.determineSize() / MB,storage.getReallocation());
        } finally {
            if (reader!=null) reader.close();
        }
    }


    public GraphStorage read(int totalNodes, long totalRels, Iterator<RelationshipRecord> rels) {
        graphStorage.init(totalNodes,totalRels);
        while (rels.hasNext()) {
            RelationshipRecord rel = rels.next();
            graphStorage.addTarget(rel.getFirstNode(), rel.getSecondNode(), rel.getType(), Direction.OUTGOING);
            graphStorage.addTarget(rel.getSecondNode(), rel.getFirstNode(), rel.getType(), Direction.INCOMING);
        }
        return graphStorage;
    }


    public GraphStorage read() {
        graphStorage.init(neoStore.getTotalNodes(), neoStore.getTotalRels());
        long totalRels = graphStorage.getTotalRels();
        long time = System.currentTimeMillis();
        for (long relId = 0; relId < totalRels; relId++) {
            RelationshipRecord rel = neoStore.rel(relId);
            graphStorage.addTarget(rel.getFirstNode(), rel.getSecondNode(), rel.getType(), Direction.OUTGOING);
            graphStorage.addTarget(rel.getSecondNode(), rel.getFirstNode(), rel.getType(), Direction.INCOMING);
            if (relId % 10_000 == 0) {
                System.out.print(".");
                if (relId % 1_000_000 == 0) {
                    System.out.println(" "+(System.currentTimeMillis()-time));
                    time = System.currentTimeMillis();
                }
            }
        }
        return graphStorage;
    }

}
