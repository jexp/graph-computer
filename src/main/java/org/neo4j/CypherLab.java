package org.neo4j;

/**
 * @author mh
 * @since 25.12.13
 */

import org.HdrHistogram.Histogram;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CypherLab {
    private static final java.util.logging.Logger LOG = java.util.logging.Logger.getLogger(CypherLab.class.getName());
    private final static int CONCURRENCY = 1000;
    public static final File DIRECTORY = new File("db/graphdb");
    private static final boolean DO_LOG = true;
    private static final int THREADS = 100;
    private static final Histogram histogram = new Histogram(1000, 4);

    public static void main(String[] args) throws Exception {
        boolean doCreate = !DIRECTORY.exists() || args.length > 0 && "create".equals(args[0]);
        if (doCreate) {
            FileUtils.deleteRecursively(DIRECTORY);
        }
        final GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder("graphdb")
                .setConfig(GraphDatabaseSettings.use_memory_mapped_buffers, "true").setConfig(GraphDatabaseSettings.cache_type, "strong")
                .newGraphDatabase();
        registerShutdownHook(graphDb);
        LOG.info("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>> NUMBER OF PARALLEL CYPHER EXECUTIONS: " + CONCURRENCY);
        LOG.info(">>>> STARTED GRAPHDB\n");
        if (doCreate) {
            createIndex("Parent", "name", graphDb);
            createIndex("Child", "name", graphDb);
            try (Transaction tx = graphDb.beginTx()) {
                Node parent = graphDb.createNode(DynamicLabel.label("Parent"));
                parent.setProperty("name", "parent");
                for (int i = 0; i < 50000; i++) {
                    Node child = graphDb.createNode(DynamicLabel.label("Child"));
                    child.setProperty("name", "child" + i);
                    parent.createRelationshipTo(child, RelationshipTypes.PARENT_CHILD);
                }
                tx.success();
            }
            LOG.info("\n>>>> CREATED NODES\n");
        }
        final ExecutionEngine engine = new ExecutionEngine(graphDb, StringLogger.SYSTEM);
        CypherRunnable warmupRunnable = new CypherRunnable(graphDb, engine, null);
        for (int i = 0; i < 10; i++) {
            warmupRunnable.run();
        }
        LOG.info("\n>>>> WARMED UP\n");
        ExecutorService es = Executors.newFixedThreadPool(THREADS);
        final CountDownLatch cdl = new CountDownLatch(CONCURRENCY);
        CypherRunnable cypherRunnable = new CypherRunnable(graphDb, engine, cdl);
        for (int i = 0; i < CONCURRENCY; i++) {
            es.execute(cypherRunnable);
        }
        cdl.await();
        es.shutdown();
        histogram.getHistogramData().outputPercentileDistribution(System.out,10,1D);
    }

    private static void createIndex(String label, String propertyName, GraphDatabaseService graphDb) {
        IndexDefinition indexDefinition;
        try (Transaction tx = graphDb.beginTx()) {
            Schema schema = graphDb.schema();
            indexDefinition = schema.indexFor(DynamicLabel.label(label)).on(propertyName).create();
            tx.success();
        }
        try (Transaction tx = graphDb.beginTx()) {
            Schema schema = graphDb.schema();
            schema.awaitIndexOnline(indexDefinition, 10, TimeUnit.SECONDS);
            tx.success();
        }
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.info("\n### GRAPHDB SHUTDOWNHOOK INVOKED !!!\n");
                graphDb.shutdown();
            }
        });
    }

    private enum RelationshipTypes implements RelationshipType {
        PARENT_CHILD
    }

    private static class CypherRunnable implements Runnable {
        private final GraphDatabaseService graphDb;
        private final ExecutionEngine engine;
        private final CountDownLatch cdl;

        public CypherRunnable(GraphDatabaseService graphDb, ExecutionEngine engine, CountDownLatch cdl) {
            this.graphDb = graphDb;
            this.engine = engine;
            this.cdl = cdl;
        }

        @Override
        public void run() {
            try (Transaction tx = graphDb.beginTx()) {
                long time = System.currentTimeMillis();
                long start = time;
                ExecutionResult result = engine.execute("match (n:Parent)-[:PARENT_CHILD]->(m:Child) return n.name, m.name");
                if (DO_LOG)
                    LOG.info(">>>> CYPHER TOOK: " + (System.currentTimeMillis() - time) + " m-secs");
                int count = 0;
                time = System.currentTimeMillis();
                for (Map<String, Object> row : result) {
                    assert ((String) row.get("n.name") != null);
                    assert ((String) row.get("m.name") != null);
                    count++;
                }
                if (DO_LOG) {
                    long now = System.currentTimeMillis();
                    LOG.info(">>>> GETTING RESULTS TOOK: " + (now - time) + " m-secs");
                    histogram.recordValue(now-start);
                }
                tx.success();
                if (DO_LOG)
                    LOG.info(">>>> CYPHER RETURNED ROWS: " + count);
            } catch (Throwable t) {
                LOG.warning(t.toString());
            } finally {
                if (cdl !=null)
                cdl.countDown();
            }
        }
    }
}
