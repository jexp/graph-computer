package org.neo4j.kernel.impl.nioneo.store.windowpool;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.*;
import org.neo4j.kernel.impl.util.StringLogger;

import java.io.File;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.nioneo.store.StoreFactory.NODE_STORE_NAME;
import static org.neo4j.kernel.impl.nioneo.store.StoreFactory.PROPERTY_STORE_NAME;
import static org.neo4j.kernel.impl.nioneo.store.StoreFactory.RELATIONSHIP_STORE_NAME;

/**
 * @author mh
 * @since 07.12.13
 */
public class DirectNeoStore implements AutoCloseable {

    public static final StringLogger LOGGER = StringLogger.SYSTEM_ERR;
    private final String path;
    private final NodeStore nodeStore;
    private final RelationshipStore relStore;
    private final PropertyStore propStore;

    public DirectNeoStore(String path) {
        this.path = path;
        DirectFileWindowPoolFactory poolFactory = new DirectFileWindowPoolFactory();
        DefaultIdGeneratorFactory idGeneratorFactory = null; // new DefaultIdGeneratorFactory();
        DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        Config config = new Config(stringMap("read_only", "true"));

        nodeStore = new NodeStore(storeFile(NODE_STORE_NAME), config, idGeneratorFactory, poolFactory, fileSystem, LOGGER, null);
        relStore = new RelationshipStore(storeFile(RELATIONSHIP_STORE_NAME), config, idGeneratorFactory, poolFactory, fileSystem, LOGGER);
        propStore = new PropertyStore(storeFile(PROPERTY_STORE_NAME), config, idGeneratorFactory, poolFactory, fileSystem, LOGGER, null, null, null);
    }

    private File storeFile(String type) {
        return new File(this.path, NeoStore.DEFAULT_NAME + type);
    }

    public void close() {
        nodeStore.close();
        relStore.close();
        propStore.close();
    }

    public NodeRecord node(long id) {
        return nodeStore.getRecord(id);
    }
    public RelationshipRecord rel(long id) {
        return relStore.getRecord(id);
    }
    public PropertyRecord prop(long id) {
        return propStore.getRecord(id);
    }

    public PropertyStore getPropStore() {
        return propStore;
    }

    public long getTotalRels() {
        return getRelStoreSize() / RelationshipStore.RECORD_SIZE;
    }

    public long getRelStoreSize() {
        return storeFile(RELATIONSHIP_STORE_NAME).length();
    }

    public long getTotalNodes() {
        return getNodeStoreSize() / NodeStore.RECORD_SIZE;
    }

    public long getNodeStoreSize() {
        return storeFile(NODE_STORE_NAME).length();
    }
}
