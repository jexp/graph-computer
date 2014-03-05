package org.neo4j;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * @author mh
 * @since 20.01.14
 */
public class TestReadAllNodes {
    @Test
    @Ignore
    public void testReadAllNodes() throws Exception {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder("/Users/mh/trash/test15M.db")
                .setConfig("cache_type", "strong")
                .setConfig("neostore.nodestore.db.mapped_memory", "200M").setConfig("use_memory_mapped_buffers", "true").newGraphDatabase();

        long sum = 0;
        long time=System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) {
            GlobalGraphOperations ops = GlobalGraphOperations.at(db);
            for (Node node : ops.getAllNodes()) {
                sum += node.getId();
            }
            tx.success();
        } finally {
            db.shutdown();
        }
        System.out.println("time = " + (System.currentTimeMillis()-time)+" ms "+sum);

    }

    @Test
    public void testReadAllNodesInParallel() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        final GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder("/Users/mh/trash/test15M.db")
                .setConfig("cache_type", "strong")
                .setConfig("neostore.nodestore.db.mapped_memory", "250M").setConfig("use_memory_mapped_buffers", "true").newGraphDatabase();

        long total = 15_000_000;
        final long segment = total / 4;
        long time=System.currentTimeMillis();

        ArrayList<Future> futures = new ArrayList<>();
        for (int i=0;i<total;i+=segment) {
            final int start = i;
            Future<Integer> future = pool.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    return loadNodes(db, segment, start);
                }
            });
            futures.add(future);
        }
        int count = 0;
        try {
            for (Future<Integer> future : futures) {
                count += future.get();
            }
            pool.shutdown();
            pool.awaitTermination(60, TimeUnit.SECONDS);
        } finally {
            db.shutdown();
        }
        System.out.println("time = " + (System.currentTimeMillis()-time)+" ms "+count);

    }

    private int loadNodes(GraphDatabaseService db, long segment, int start) {
        int count=0;
        try (Transaction tx = db.beginTx()) {
            for (int id= start; id< start + segment;id++) {
                try {
                    Node node = db.getNodeById(id);
                    count++;
                } catch(NotFoundException nfe) {}
            }
            tx.success();
        }
        return count;
    }
}
