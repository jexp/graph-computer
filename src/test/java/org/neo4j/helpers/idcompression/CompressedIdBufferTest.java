package org.neo4j.helpers.idcompression;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CompressedIdBufferTest
{
    private ByteBuffer byteBuffer;
    private CompressedIdBuffer idBuffer;

    @Before
    public void setUp() throws Exception
    {
        byteBuffer = BufferType.SMALL.allocateBuffer();
        idBuffer = new CompressedIdBuffer( byteBuffer, true );
    }

    @Test
    public void testStoreSingleValue() throws Exception
    {
        assertStoreAndRead( 42 );
    }

    @Test
    public void testStoreTwoDifferentValues() throws Exception
    {
        assertStoreAndRead( 42, 128 );
    }

    @Test
    public void testStoreFiveDifferentValues() throws Exception
    {
        assertStoreAndRead( 42, 128, 23567465, 854646, 5646878 );
    }

    @Test
    public void testStoreBoundaryConditions() throws Exception
    {
        assertStoreAndRead( 0, 0x7F, 0xFF, 0x01FF, 0xFFFF );
    }

    @Test
    public void testStoreDuplicates() throws Exception
    {
        assertStoreAndRead( 42, 42, 42, 42, 42, 42 );
    }

    @Test
    public void testStoreMixedDuplicates() throws Exception
    {
        assertStoreAndRead( 42, 42, 128, 42, 42, 12, 42, 42, 42, 10 );
    }

    private void assertStoreAndRead( long... data )
    {
        for ( long value : data )
        {
            idBuffer.store( value );
        }
        idBuffer.flush();
        System.out.println( "idBuffer = " + idBuffer + " for " + Arrays.toString( data ) );
        idBuffer.toggleMode();
        for ( long value : data )
        {
            assertEquals( value, idBuffer.read() );
        }
    }

    @Test
    public void testPerformance() throws Exception
    {
        CompressedIdBuffer buffer = new CompressedIdBuffer( BufferType.LARGE, true );
        int counter=0;
        long t = System.currentTimeMillis();
        for ( long i = 0; i < 10000000; i++ )
        {
            long value = i % 10000;
            if ( value % 10 == 0 )
            {
                for ( int j = 0; j < 100; j++ )
                {
                    buffer.store( value );
                }
                counter+=100;
            } else
            {
                buffer.store( value );
                counter ++;
            }
        }
        buffer.flush();
        t = System.currentTimeMillis() - t;
        System.out.println( "Stored "+counter+" values in "+t+" ms. "+buffer+ " bytes/value "+buffer.getBytesPerValue());
    }
}
