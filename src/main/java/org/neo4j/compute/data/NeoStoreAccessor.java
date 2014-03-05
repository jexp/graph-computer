package org.neo4j.compute.data;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.StoreLocker;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.*;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SingleLoggingService;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mh
 * @since 29.11.13
 */
public class NeoStoreAccessor {

    private File storeDir;
    private NeoStore neoStore;
    private boolean isShutdown;
    private StoreLocker storeLocker;
    private StringLogger msgLog;

    void init(String storeDir, Map<String, String> stringParams) {
        FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        this.storeDir = new File( FileUtils.fixSeparatorsInPath(storeDir) );

        msgLog = StringLogger.SYSTEM_ERR;
        Logging logging = new SingleLoggingService(msgLog);
        Map<String, String> params = getDefaultParams();
        params.put( GraphDatabaseSettings.use_memory_mapped_buffers.name(), Settings.FALSE );
        params.put( InternalAbstractGraphDatabase.Configuration.store_dir.name(), storeDir );
        params.putAll( stringParams );

        storeLocker = new StoreLocker( fileSystem );
        storeLocker.checkLock(this.storeDir);

        Config config = new Config(params, GraphDatabaseSettings.class);
        boolean dump = config.get( GraphDatabaseSettings.dump_configuration );
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();

        StoreFactory sf = new StoreFactory( config, idGeneratorFactory, new DefaultWindowPoolFactory(), fileSystem,
                msgLog, null );


        msgLog.logMessage(Thread.currentThread() + " Starting BatchInserter(" + this + ")");
        neoStore = sf.newNeoStore(new File(storeDir, NeoStore.DEFAULT_NAME));
        if ( !neoStore.isStoreOk() )
        {
            throw new IllegalStateException( storeDir + " store is not cleanly shutdown." );
        }

    }

    private Map<String, String> getDefaultParams()
    {
        Map<String, String> params = new HashMap<>();
        params.put( "neostore.nodestore.db.mapped_memory", "20M" );
        params.put( "neostore.propertystore.db.mapped_memory", "90M" );
        params.put( "neostore.propertystore.db.index.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.index.keys.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.strings.mapped_memory", "130M" );
        params.put( "neostore.propertystore.db.arrays.mapped_memory", "130M" );
        params.put( "neostore.relationshipstore.db.mapped_memory", "50M" );
        return params;
    }

    public void shutdown()
    {
        if ( isShutdown )
        {
            throw new IllegalStateException( "Batch inserter already has shutdown" );
        }
        isShutdown = true;

        neoStore.close();

        try
        {
            storeLocker.release();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Could not release store lock", e );
        }

        msgLog.logMessage(Thread.currentThread() + " Clean shutdown on BatchInserter(" + this + ")", true);
        msgLog.close();
    }
}
