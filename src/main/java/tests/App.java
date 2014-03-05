package tests;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class App
{
    // Each run, we'll add 1,00,000 nodes, in discrete star subgraphs.
    // Each "record" is at the centre of a subgraph, surrounded by 9
    // "subrecords".  Each node has 10 properties with random content.
    static final int RECORDS = 1_000_000;
    static final int SUBRECORDS = 9;
    static final int PROPERTIES = 10;
    static final int COMMIT_EVERY = 50000;
    static final int RUNS = 1;
    private final static String BATCH_DATABASE = "./db/n4j-batch-db";
    private final static String TRANS_DATABASE = "./db/n4j-transactional-db";

    public static void main( String[] args ) throws Exception {

        batchInsert(BATCH_DATABASE,COMMIT_EVERY*20);

//        transactionalInsert(TRANS_DATABASE,COMMIT_EVERY);
    }

    private static void transactionalInsert(String database, int commitEvery) throws IOException {
        FileUtils.deleteRecursively(new File(database));
        final long startTime = System.nanoTime();

        int run = 0;
        int nodesAddedSinceLastCommit = 0;
        int totalNodesAdded = 0;

        while(run++ < RUNS)
        {
            int runNodesAdded = 0;
            final long runStartTime = System.nanoTime();
            GraphDatabaseService graph = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(database).setConfig(config()).newGraphDatabase();
            Transaction transaction = graph.beginTx();

            for(int record = 0; record < RECORDS; ++record)
            {
                Node recordNode = graph.createNode();
                nodesAddedSinceLastCommit++;
                totalNodesAdded++;
                runNodesAdded++;

                for (Map.Entry<String, Object> entry : createProperties(PROPERTIES).entrySet()) {
                    recordNode.setProperty(entry.getKey(), entry.getValue());
                }

                for(long subrecord = 0; subrecord < SUBRECORDS; ++subrecord)
                {
                    Node subRecordNode = graph.createNode();
                    nodesAddedSinceLastCommit++;
                    totalNodesAdded++;
                    runNodesAdded++;

                    for (Map.Entry<String, Object> entry : createProperties(PROPERTIES).entrySet()) {
                        subRecordNode.setProperty(entry.getKey(), entry.getValue());
                    }

                    // Each record is linked to and from each of its subrecords.
                    // This dual linking is possibly not necessary, I'm including
                    // it because this is how I got my original throughput numbers.
                    RelationshipType toChild = DynamicRelationshipType.withName("child");
                    recordNode.createRelationshipTo(subRecordNode, toChild);

                    RelationshipType toParent = DynamicRelationshipType.withName("parent");
                    subRecordNode.createRelationshipTo(recordNode, toParent);
                } // end of adding subrecords

                if(nodesAddedSinceLastCommit >= commitEvery || record + 1 == RECORDS) {
                    nodesAddedSinceLastCommit = 0;
                    transaction.success();
                    transaction.finish();
                    transaction = graph.beginTx();

                    final long totalElapsedNanoseconds = System.nanoTime() - startTime;
                    final double totalElapsedSeconds = totalElapsedNanoseconds / 1000_000_000.0;
                    final double totalNodesPerSecond = totalNodesAdded / totalElapsedSeconds;

                    final long runElapsedNanoseconds = System.nanoTime() - runStartTime;
                    final double runElapsedSeconds = runElapsedNanoseconds / 1000_000_000.0;
                    final double runNodesPerSecond = runNodesAdded / runElapsedSeconds;

                    System.out.println(totalNodesAdded + "\t" + totalElapsedSeconds + "\t" + totalNodesPerSecond + "\t" +
                            runNodesAdded + "\t" + runElapsedSeconds + "\t" + runNodesPerSecond);
                }
            } // end of adding records

            transaction.success();
            transaction.finish();
            graph.shutdown();

            System.out.println("------------------------------------------");
        } // end of runs
    }

    private static void batchInsert(String database, int commitEvery) throws IOException {

        FileUtils.deleteRecursively(new File(database));
        final long startTime = System.nanoTime();

        int nodesAddedSinceLastCommit = 0;
        int totalNodesAdded = 0;

        int run = 0;
        while(run++ < RUNS)
        {
            int runNodesAdded = 0;
            final long runStartTime = System.nanoTime();
            final BatchInserter graph = BatchInserters.inserter(database, config());

            for(int record = 0; record < RECORDS; ++record)
            {
                // Dynamic labels are used in anticipation of a general purpose loader applicatin

                long recordNode = graph.createNode(createProperties(PROPERTIES));//, recordLabel);
                nodesAddedSinceLastCommit++;
                totalNodesAdded++;
                runNodesAdded++;

                for(long subrecord = 0; subrecord < SUBRECORDS; ++subrecord)
                {

                    long subRecordNode = graph.createNode(createProperties(PROPERTIES));//, subRecordLabel);

                    nodesAddedSinceLastCommit++;
                    totalNodesAdded++;
                    runNodesAdded++;

                    // Each record is linked to and from each of its subrecords.
                    // This dual linking is possibly not necessary, I'm including
                    // it because this is how I got my original throughput numbers.
                    RelationshipType toChild = DynamicRelationshipType.withName("child");
                    graph.createRelationship(recordNode, subRecordNode, toChild, null);

                    RelationshipType toParent = DynamicRelationshipType.withName("parent");
                    graph.createRelationship(subRecordNode, recordNode, toParent, null);
                } // end of adding subrecords
                if(nodesAddedSinceLastCommit >= commitEvery || record + 1 == RECORDS) {
                    nodesAddedSinceLastCommit = 0;

                    final long totalElapsedNanoseconds = System.nanoTime() - startTime;
                    final double totalElapsedSeconds = totalElapsedNanoseconds / 1000000000.0;
                    final double totalNodesPerSecond = totalNodesAdded / totalElapsedSeconds;

                    final long runElapsedNanoseconds = System.nanoTime() - runStartTime;
                    final double runElapsedSeconds = runElapsedNanoseconds / 1000000000.0;
                    final double runNodesPerSecond = runNodesAdded / runElapsedSeconds;

                    System.out.println(totalNodesAdded + "\t" + totalElapsedSeconds + "\t" + totalNodesPerSecond + "\t" +
                            runNodesAdded + "\t" + runElapsedSeconds + "\t" + runNodesPerSecond);
                }
            } // end of adding records

            final long elapsedNanosecondsBeforeShutdown = System.nanoTime() - startTime;
            final double elapsedSecondsBeforeShutdown = elapsedNanosecondsBeforeShutdown / 1000000000.0;

            graph.shutdown();

            final long elapsedNanosecondsAfterShutdown = System.nanoTime() - startTime;
            final double elapsedSecondsAfterShutdown = elapsedNanosecondsAfterShutdown / 1000000000.0;

            System.out.println(run + "\t" + elapsedSecondsBeforeShutdown + "\t" + elapsedSecondsAfterShutdown);

        } // end of runs
    }

    private static Map<String, String> config() {
        return stringMap(
                "use_memory_mapped_buffers", "true",
                "neostore.nodestore.db.mapped_memory", "1G",
                "neostore.relationshipstore.db.mapped_memory", "2G",
                "neostore.propertystore.db.mapped_memory", "500M",
                "neostore.propertystore.db.strings.mapped_memory", "200M",
                "neostore.propertystore.db.arrays.mapped_memory", "0M",
                "neostore.propertystore.db.index.keys.mapped_memory", "15M",
                "neostore.propertystore.db.index.mapped_memory", "15M",
                "cache_type","none",
                "dump_config","true");
    }

    private static Map<String,Object> createProperties(int properties) {
        Map<String,Object> propertiesMap = new HashMap<>();
        for(int property = 0; property < properties; ++property)
        {
            String key = "property" + property;
//            String value = Double.toString(Math.random());
            String value = key; //Double.toString(Math.random());

            propertiesMap.put(key, value);
        }
        return propertiesMap;
    }

}
