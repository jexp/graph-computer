package org.neo4j;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * @author mh
 * @since 12.02.14
 */
public class CachePollutionTest {

    @Test
    public void testCachePolution() throws Exception {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Node n;
        try (Transaction tx = db.beginTx()) {
            n = db.createNode();
            tx.success();
        }
        try (Transaction tx = db.beginTx()) {
            try {
            } catch (IllegalStateException ise) {
                System.out.println("first delete " +ise.getMessage());
            }
            tx.success();
        }
        try (Transaction tx = db.beginTx()) {
            try {
                n.delete();
                for (Node node : GlobalGraphOperations.at(db).getAllNodes()) {
                    node.delete();
                }
            } catch (IllegalStateException ise) {
                System.out.println("second delete "+ise.getMessage());
            }
            tx.success();
        }
    }
}
