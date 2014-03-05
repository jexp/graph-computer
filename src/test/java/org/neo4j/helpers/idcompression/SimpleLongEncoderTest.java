/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.helpers.idcompression;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

// MSB -----> LSB
public class SimpleLongEncoderTest
{
    @Test
    public void shouldEncodeAndDecodeOneByteValue() throws Exception
    {
        assertEncodeAndDecodeValue( 0, bytes( 0        ) );
        assertEncodeAndDecodeValue( 1, bytes( 1,     1 ) );
        assertEncodeAndDecodeValue( 10, bytes( 10,   1 ) );
        assertEncodeAndDecodeValue( 34, bytes( 34,   1 ) );
        assertEncodeAndDecodeValue( 127, bytes( 127, 1 ) );
        assertEncodeAndDecodeValue( -1, bytes( 1,   -1 ) );
    }

    @Test
    public void shouldEncodeAndDecodeTwoByteValue() throws Exception
    {
        assertEncodeAndDecodeValue( 0x80, // 128
                             bytes( 0x80, 1 ) );
        assertEncodeAndDecodeValue( 0x0442, // 1234
                             bytes( 0x04, 0x42, 2 ) );
        assertEncodeAndDecodeValue( 0x3FFF, // 2^15-1
                             bytes( 0x3F, 0xFF, 2 ) );
        assertEncodeAndDecodeValue( -128, // 128
                             bytes( 0x80, -1 ) );
    }
    
    @Test
    public void shouldEncodeAndDecodeThreeByteValue() throws Exception
    {
        assertEncodeAndDecodeValue( 0x16B1B2,
                             bytes( 0x16, 0xB1, 0xB2,  3 ) );
        assertEncodeAndDecodeValue( - 0x16B1B2,
                             bytes( 0x16, 0xB1, 0xB2, -3 ) );
    }
    
    @Test
    public void shouldEncodeAndDecodeEightBytes() throws Exception
    {
        assertEncodeAndDecodeValue( 0x0081020408102040L,
                             bytes( 0x81,0x02,0x04,0x08,0x10,0x20,0x40, 7 ) );
        assertEncodeAndDecodeValue( 0x7FFFFFFFFFFFFFFFL,
                bytes( 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 8 ) );
        assertEncodeAndDecodeValue( - 0x7FFFFFFFFFFFFFFFL,
                bytes( 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, -8 ) );
        assertEncodeAndDecodeValue( 0x0000000000000000L,
                bytes( 0 ) );
    }
    
    @Test
    public void testPerformance() throws Exception
    {
        int idCount = 10000000;
        ByteBuffer buffer = ByteBuffer.allocate( idCount*4 );
        for ( int i = 0; i < 100; i++ )
            timeEncodeDecode( buffer, 10000000, 100000 );
    }

    private void timeEncodeDecode( ByteBuffer buffer, int idCount, int mod )
    {
        // ENCODE
        buffer.clear();
        SimpleLongEncoder encoder = new SimpleLongEncoder();
        long t = System.currentTimeMillis();
        for ( int i = 0; i < idCount; i++ )
        {
            encoder.encode( buffer, (i%mod)+1 );
        }
        t = System.currentTimeMillis()-t;
        System.out.println( idCount/t + " values/ms " + idCount + " in " + t + "ms " + buffer.position() + " bytes used, avg " + ((float)buffer.position()/idCount) + " bytes/id" );
        
        // DECODE
        buffer.flip();
        t = System.currentTimeMillis();
        for ( int i = 0; i < idCount; i++ )
        {
            encoder.decode( buffer );
        }
        t = System.currentTimeMillis()-t;
        System.out.println( idCount/t + " values/ms " + idCount + " in " + t + "ms" );
    }
    
    private void assertEncodeAndDecodeValue( long value, byte[] expectedEncodedBytes )
    {
        // ENCODE
        byte[] array = new byte[expectedEncodedBytes.length];
        ByteBuffer buffer = ByteBuffer.wrap( array );
        SimpleLongEncoder encoder = new SimpleLongEncoder();

        int bytesUsed = encoder.encode( buffer, value );

        assertEquals( expectedEncodedBytes.length, bytesUsed );
        assertArrayEquals( expectedEncodedBytes, array );
        
        // DECODE
        buffer.flip();
        assertEquals( value, encoder.decode( buffer ) );
    }
    
    private byte[] bytes( long... values )
    {
        byte[] result = new byte[values.length];
        for ( int i = 0; i < values.length; i++ )
            result[i] = (byte) values[values.length-i-1];
        return result;
    }

    private byte[] bytes( int... values )
    {
        byte[] result = new byte[values.length];
        for ( int i = 0; i < values.length; i++ )
            result[i] = (byte) values[values.length-i-1];
        return result;
    }
}
