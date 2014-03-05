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
package org.neo4j.helpers.idcompression.bytes;

import org.junit.Test;
import org.neo4j.helpers.idcompression.SignedLongBase128Encoder;
import org.neo4j.helpers.idcompression.bytes.SignedLongBase128EncoderBytes;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

// MSB -----> LSB
public class SignedLongBase128EncoderBytesTest
{
    @Test
    public void shouldEncodeAndDecodeOneByteValue() throws Exception
    {
        assertEncodeAndDecodeValue( 0, bytes( 0 ) );
        assertEncodeAndDecodeValue( 1, bytes( 1 ) );
        assertEncodeAndDecodeValue( 10, bytes( 10 ) );
        assertEncodeAndDecodeValue( 34, bytes( 34 ) );
        assertEncodeAndDecodeValue( 127, bytes( 0x00, 0xFF ) );
        assertEncodeAndDecodeValue( -1, bytes( 0x41 ) );
    }

    @Test
    public void shouldEncodeAndDecodeTwoByteValue() throws Exception
    {
        assertEncodeAndDecodeValue( 0x80, // 128
                             bytes( 0x01, 0x80 ) );
        assertEncodeAndDecodeValue( 0x0442, // 1234
                             bytes( 0x08, 0xC2 ) );
        assertEncodeAndDecodeValue( 0x3FFF, // 2^15-1
                             bytes( 0x00, 0xFF, 0xFF ) );
        assertEncodeAndDecodeValue( -128, // 128
                             bytes( 0x41, 0x80 ) );
    }
    
    @Test
    public void shouldEncodeAndDecodeThreeByteValue() throws Exception
    {
        assertEncodeAndDecodeValue( 0x16B1B2,
                             bytes( 0x00, 0xDA, 0xE3, 0xB2 ) );
        assertEncodeAndDecodeValue( - 0x16B1B2,
                             bytes( 0x40, 0xDA, 0xE3, 0xB2 ) );
    }
    
    @Test
    public void shouldEncodeAndDecodeEightBytes() throws Exception
    {
        assertEncodeAndDecodeValue( 0x0000000000000000L,
                             bytes( 0 ) );
        assertEncodeAndDecodeValue( 0x0081020408102040L,
                             bytes( 0x00, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0, 0xC0 ) );
        assertEncodeAndDecodeValue( 0x7FFFFFFFFFFFFFFFL,
                bytes( 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF ) );
        assertEncodeAndDecodeValue( - 0x7FFFFFFFFFFFFFFFL,
                bytes( 0x40, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF ) );
    }
    
    @Test
    public void testPerformance() throws Exception
    {
        int idCount = 10000000;
        ByteBuffer buffer = ByteBuffer.allocate( idCount*4 );
        for ( int i = 0; i < 10; i++ )
            timeEncodeDecode( buffer, 10000000, 100000 );
    }

    private void timeEncodeDecode( ByteBuffer buffer, int idCount, int mod )
    {
        // ENCODE
        buffer.clear();
        SignedLongBase128Encoder encoder = new SignedLongBase128Encoder();
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
        byte[] buffer = new byte[expectedEncodedBytes.length];
        SignedLongBase128EncoderBytes encoder = new SignedLongBase128EncoderBytes();

        int bytesUsed = encoder.encode( buffer, 0, value );

        assertEquals( expectedEncodedBytes.length, bytesUsed );
        assertArrayEquals( expectedEncodedBytes, buffer );
        
        // DECODE
        assertEquals( value, encoder.decode( buffer, 0 ) );
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
