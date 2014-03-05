package org.neo4j.helpers.idcompression;

import java.nio.ByteBuffer;

/**
 * Buffer sizes used in {@link LinkBlock} and {@link CompressedIdBuffer}. 
 */
public enum BufferType
{
    // TODO think about sizes
    SMALL( 1024 ),
    MEDIUM( 1024*1024 ),
    LARGE( 128*1024*1024 ),
    HUGE( 1024*1024*1024 );

    private final int byteSize;

    BufferType( int byteSize )
    {
        this.byteSize = byteSize;
    }

    int byteSize()
    {
        return byteSize;
    }

    public final static int HEADER = 4;

    ByteBuffer allocateBuffer()
    {
        ByteBuffer result = ByteBuffer.allocate( byteSize );
        result.putInt( byteSize() );
        return result;
    }
}
