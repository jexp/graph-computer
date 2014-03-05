package org.neo4j;

import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * @author mh
 * @since 19.12.13
 */
public class CreateLargeNodeStore {
    public static final int MILLION = 1000 * 1000;
    public static final int COUNT = 600 * MILLION;
    public static final String PATH = "db/large.db"+"_"+COUNT;

    enum Labels implements Label {
        ONE, TWO, THREE
    }

    public static void main(String[] args) throws IOException {
        FileUtils.deleteRecursively(new File(PATH));
        BatchInserter inserter = BatchInserters.inserter(PATH, config());
        Labels[] labels = Labels.values();
        long time = System.currentTimeMillis();
        try {
            for (int i = 0; i < COUNT; i++) {
                inserter.createNode(map("name", String.valueOf(i % MILLION), "age", i, "address", "street " + i + " city " + i + " nowhere"), labels[i % 3]);
                if (i % MILLION == 0) {
                    time = trace(i + " nodes ", time);
                }
            }
        } finally {
            time = trace("before shutdown ", time);
            inserter.shutdown();
        }
        trace("after shutdown", time);
    }

    private static long trace(Object msg, long time) {
        long now = System.currentTimeMillis();
        System.out.println("" + msg + " time " + (now - time) + " ms");
        return now;
    }

    private static Map<String, String> config() {
        return stringMap(
                "use_memory_mapped_buffers", "true",
                "neostore.nodestore.db.mapped_memory", "2G",
                "neostore.relationshipstore.db.mapped_memory", "0M",
                "neostore.propertystore.db.mapped_memory", "1G",
                "neostore.propertystore.db.strings.mapped_memory", "1G",
                "neostore.propertystore.db.arrays.mapped_memory", "0M",
                "neostore.propertystore.db.index.keys.mapped_memory", "1M",
                "neostore.propertystore.db.index.mapped_memory", "1M",
                "cache_type", "none",
                "dump_config", "true");
    }

}

