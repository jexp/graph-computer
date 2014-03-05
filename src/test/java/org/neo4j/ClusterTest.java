package org.neo4j;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author mh
 * @since 19.01.14
 */
public class ClusterTest {

    public static final DynamicRelationshipType SIMILAR = DynamicRelationshipType.withName("SIMILAR");
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    /*
            A -> B,
        A -> C,
        D -> B,
        E -> F,
        Z
             */
    @Test
    public void testSimpleCluster() throws Exception {
        createData();
        try (Transaction tx = db.beginTx()) {
            TraversalDescription traversal = db.traversalDescription().depthFirst().uniqueness(Uniqueness.NODE_GLOBAL);
            Map<Node, Set<Node>> clusters = new HashMap<>();
            GlobalGraphOperations ops = GlobalGraphOperations.at(db);
            for (Node node : ops.getAllNodes()) {
                if (inCluster(node, clusters) != null) continue;
                clusters.put(node, IteratorUtil.addToCollection(traversal.traverse(node).nodes(), new HashSet<Node>()));
            }
            System.out.println("clusters = " + clusters.values());
            tx.success();
        }
    }

    @Test
    public void testSimpleClusterCypher() throws Exception {
        createData();
        ExecutionEngine engine = new ExecutionEngine(db);
        ExecutionResult result = engine.execute("MATCH m\n" +
                "WITH collect(m) as all\n" +
                "MATCH n\n" +
                "RETURN distinct [x in all WHERE (n)-[*0..]-(x) | x.name] as cluster");
        for (Map<String, Object> row : result) {
            System.out.println("row = " + row);
        }
    }
    @Test
    public void testSimpleClusterCypher2() throws Exception {
        createData();
        ExecutionEngine engine = new ExecutionEngine(db);
        ExecutionResult result = engine.execute("MATCH n-[*0..]-m \n" +
                "WITH n, m "+
                "ORDER BY id(m) "+
                "WITH n, collect(m.name) as cluster\n" +
                "RETURN distinct cluster");
        for (Map<String, Object> row : result) {
            System.out.println("row = " + row);
        }
    }

    private Node inCluster(Node node, Map<Node, Set<Node>> clusters) {
        for (Map.Entry<Node, Set<Node>> entry : clusters.entrySet()) {
            if (entry.getValue().contains(node)) return entry.getKey();
        }
        return null;
    }

    private void createData() {
        try (Transaction tx = db.beginTx()) {
            Node a = node("a");
            Node b = node("b");
            Node c = node("c");
            Node d = node("d");
            Node e = node("e");
            Node f = node("f");
            Node z = node("z");
            connect(a, b);
            connect(a, c);
            connect(d, b);
            connect(e, f);
            tx.success();
        }
    }

    private void connect(Node a, Node b) {
        a.createRelationshipTo(b, SIMILAR);
    }

    private Node node(String name) {
        Node node = db.createNode();
        node.setProperty("name", name);
        return node;
    }
}
