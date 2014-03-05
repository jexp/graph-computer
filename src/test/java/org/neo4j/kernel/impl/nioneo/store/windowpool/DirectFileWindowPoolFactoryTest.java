package org.neo4j.kernel.impl.nioneo.store.windowpool;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.nioneo.store.*;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 07.12.13
 */
public class DirectFileWindowPoolFactoryTest {

    public static final String PATH = "db/direct-test.db";
    private static final int COUNT =  50_000_000; // 50_000_000; // 4.1 GB  , create 126s, read 18s
    public static final String VALUE = "bar";
    public static final String KEY = "foo";
    public static final Map<String,Object> PROPS = map(KEY, VALUE);
    public static final DynamicRelationshipType RELATIONSHIP_TYPE = DynamicRelationshipType.withName("FOO");
    public static final Map<String, Object> NO_PROPS = Collections.emptyMap();

    @Test
    @Ignore
    public void testCreateDb() throws Exception {
        FileUtils.deleteRecursively(new File(PATH));
        long time = System.currentTimeMillis();
        createTestDb();
        System.out.println("Create data took "+(System.currentTimeMillis() - time)+" ms");
    }

    @Test
    public void testReadNodeRecord() throws Exception {
        try (DirectNeoStore store = new DirectNeoStore(PATH)) {
            long time = System.currentTimeMillis();
            for (int i = 0; i < COUNT; i++) {
                NodeRecord record = store.node(i);
//                assertEquals(i, record.getNextRel());
//                assertEquals(i, record.getNextProp());

                RelationshipRecord relRecord = store.rel(i);
//                assertEquals(0, relRecord.getType());
//                assertEquals(-1, relRecord.getFirstNextRel());
//                assertEquals(-1, relRecord.getSecondNextRel());
//                assertEquals(-1, relRecord.getFirstPrevRel());
//                assertEquals(-1, relRecord.getSecondPrevRel());
//                assertEquals(i, relRecord.getFirstNode());
//                assertEquals(i, relRecord.getSecondNode());
//                assertEquals(-1, relRecord.getNextProp());

                PropertyRecord propRecord = store.prop(i);
//                assertEquals(-1, propRecord.getNextProp());
//                PropertyBlock block = propRecord.getPropertyBlock(0);
//                assertEquals(0, block.getKeyIndexId());
//                assertEquals(PropertyType.SHORT_STRING, block.getType());
//                DefinedProperty property = PropertyType.SHORT_STRING.readProperty(0, block, store.getPropStore());
//                assertEquals(VALUE, property.value());
            }
            System.out.println("Read data took " + (System.currentTimeMillis() - time) + " ms");
        }
    }

    private void createTestDb() {
        BatchInserter db = BatchInserters.inserter(PATH);
            for (int i=0;i<COUNT;i++) {
                long node = db.createNode(PROPS);
                db.createRelationship(node, node, RELATIONSHIP_TYPE, NO_PROPS);
            }
        db.shutdown();
    }
}
