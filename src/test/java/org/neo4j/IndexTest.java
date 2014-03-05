package org.neo4j;

import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 08.01.14
 */
public class IndexTest {

    public static final Label LABEL = DynamicLabel.label("Label");

    @Test
    public void testAddRemoveIndex() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try (Transaction tx = db.beginTx()) {
            Index<Node> index = db.index().forNodes("nodes");
            Node node = db.createNode();
            node.setProperty("foo","bar");
            index.add(node, "foo", "bar");

            node.delete();
            IndexHits<Node> result = index.get("foo", "bar");
            ResourceIterator<Node> iterator = result.iterator();
            Node actual = IteratorUtil.singleOrNull(iterator);
            actual.hasProperty("foo");
            assertEquals(null, actual);
            tx.success();
        }
        db.shutdown();
    }
    @Test
    public void testAddRemoveIndex2Tx() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        try (Transaction tx = db.beginTx()) {
            Index<Node> index = db.index().forNodes("nodes");
            Node node = db.createNode();
            node.setProperty("foo","bar");
            index.add(node, "foo", "bar");

            node.delete();
            tx.success();
        }
        try (Transaction tx = db.beginTx()) {
            Index<Node> index = db.index().forNodes("nodes");
            IndexHits<Node> result = index.get("foo", "bar");
            assertEquals(0, IteratorUtil.count(result.iterator()));
            tx.success();
        }
        db.shutdown();
    }
    @Test
    public void testDeletedNodeOps() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        long nodeId = 0;
        try (Transaction tx = db.beginTx()) {
            Node node = db.createNode(LABEL);
            node.setProperty("foo","bar");
            nodeId = node.getId();
            tx.success();
        }
        try (Transaction tx = db.beginTx()) {
            Node node = db.getNodeById(nodeId);

            node.delete();

            assertEquals(false,node.hasLabel(LABEL));
            assertEquals(0,IteratorUtil.count(node.getLabels()));
//            assertEquals(false,node.hasProperty("foo"));
//            assertEquals("bar",node.getProperty("foo"));
            tx.success();
        }
        db.shutdown();
    }

    @Test
    public void testCypherExecutionEngine() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        for (int i=0;i<100;i++) {
            ExecutionEngine engine = new ExecutionEngine(db);
            ExecutionResult result = engine.execute("MERGE (ref:ReferenceNode {name:{name}}) RETURN ref",map("name", "root"));
            Node node = IteratorUtil.single(result.<Node>columnAs("ref"));
        }
        db.shutdown();
    }
}
