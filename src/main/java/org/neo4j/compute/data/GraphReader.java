package org.neo4j.compute.data;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mh
 * @since 29.11.13
 */
public class GraphReader {

    public static final int INITIAL_ENTRIES = 64;
    public static final int MB = 1024 * 1024;
    private final BatchInserter inserter;
    private final String storeDir;
    private final File nodeStoreFile;
    private final int totalNodes; // todo Long
    private File baseStoreFile;
    private File relationshipStoreFile;
    private final long totalRels;
    private int[][] nodes;

    public GraphReader(String storeDir) {
        this.storeDir = storeDir;
        relationshipStoreFile = new File(storeDir, NeoStore.DEFAULT_NAME + StoreFactory.RELATIONSHIP_STORE_NAME);
        nodeStoreFile = new File(storeDir, NeoStore.DEFAULT_NAME + StoreFactory.NODE_STORE_NAME);
        totalRels = relationshipStoreFile.length() / RelationshipStore.RECORD_SIZE;
        totalNodes = (int) (nodeStoreFile.length() / NodeStore.RECORD_SIZE);
        System.out.printf("store %s nodes %dMB -> %d rels %dMB -> %d%n", storeDir, nodeStoreFile.length() / MB, totalNodes,
                                                                                 relationshipStoreFile.length() / MB, totalRels);
        inserter = BatchInserters.inserter(this.storeDir, config());
    }

    private int measureReadFile(File file) throws IOException {
        byte[] buffer = new byte[MB*16];
        BufferedInputStream is = new BufferedInputStream(new FileInputStream(file), MB);
        int read;
        int count=0, sum = 0;
        long time = System.currentTimeMillis();
        while ((read = is.read(buffer))!=-1) {
            for (int i=0;i<read;i++) {
                count++;
                sum += buffer[i];
            }
        }
        is.close();
        System.out.printf("reading %s time %d ms, size %d MB%n", file, (System.currentTimeMillis() - time), count / MB);
        return sum;
    }

    public void close() {
        inserter.shutdown();
    }

    public static void main(String[] args) throws IOException {
        GraphReader reader = null;
        try {
            reader = new GraphReader(args[0]);
            reader.measureReadRelFile();
            long time=System.currentTimeMillis();
            int[][] nodes = reader.read();
            System.out.printf("time %d s, size %d MB%n", (System.currentTimeMillis() - time) / 1000, determineSize(nodes) / MB);
        } finally {
            if (reader!=null) reader.close();
        }
    }

    private void measureReadRelFile() throws IOException {
        measureReadFile(relationshipStoreFile);
    }


    private static int determineSize(int[][] nodes) throws IOException {
        CountingOutputStream counter = new CountingOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(counter);
        os.writeObject(nodes);
        os.close();
        return counter.getCount();
    }

    private int[][] read() {
        this.nodes = new int[totalNodes][INITIAL_ENTRIES];
        for (long relId = 0; relId < totalRels; relId++) {
            BatchRelationship rel = inserter.getRelationshipById(relId);
            addTarget(rel.getStartNode(), rel.getEndNode(), rel.getType(), Direction.OUTGOING);
            addTarget(rel.getEndNode(), rel.getStartNode(), rel.getType(), Direction.INCOMING);
        }
        return nodes;
    }

    private void addTarget(long nodeId, long target, RelationshipType type, Direction direction) {
        int arrayOffset = (int) nodeId;
        int[] node = nodes[arrayOffset];
        int entryOffset = node[0] + 1;
        if (entryOffset == node.length) {
            nodes[arrayOffset] = node = Arrays.copyOf(node, node.length * 2);
        }
        node[0] = entryOffset;
        node[entryOffset] = (int) target; // combine with type and direction
    }

    private Map<String, String> config() {
        return getDefaultParams();
    }


    private Map<String, String> getDefaultParams() {
        Map<String, String> params = new HashMap<>();
        params.put("neostore.nodestore.db.mapped_memory", "50M");
        params.put("neostore.propertystore.db.mapped_memory", "500M");
        params.put("neostore.propertystore.db.index.mapped_memory", "1M");
        params.put("neostore.relationshipstore.db.mapped_memory", "1G");
        params.put("neostore.propertystore.db.index.keys.mapped_memory", "1M");
        params.put("neostore.propertystore.db.strings.mapped_memory", "250M");
        params.put("neostore.propertystore.db.arrays.mapped_memory", "250M");
        return params;
    }

    private static class CountingOutputStream extends OutputStream {
        int count = 0;

        public void write(int b) throws IOException {
            count++;
        }

        public int getCount() {
            return count;
        }
    }
}
