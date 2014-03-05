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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class LinkBlockTest
{
    @Test
    public void setSinglePair() throws Exception
    {
        // GIVEN
        LinkBlock block = new LinkBlock( BufferType.SMALL );
        long[][] expected = idPairs( 10, 10, 1 );
        block.set( expected );
        
        // WHEN
        long[][] read = new long[1][2];
        block.get( read );

        // THEN
        assertArrayEquals( expected[0], read[0] );
    }
    
    @Test
    public void setManyPairs() throws Exception
    {
        // GIVEN
        LinkBlock block = new LinkBlock( BufferType.LARGE );
        long[][] expected = idPairs( 5, 10, 10000 );
        block.set( expected );
        System.out.println( block );
        
        // WHEN
        long[][] read = new long[expected.length][2];
        block.get( read );
        
        for ( int i = 0; i < read.length; i++ )
        {
            assertArrayEquals( "At index " + i, expected[i], read[i] );
        }
    }

    @Test
    public void setManyNegativeDeltaPairs() throws Exception
    {
        // GIVEN
        LinkBlock block = new LinkBlock( BufferType.LARGE );
        long[][] expected = idPairs( 10, 5, 10000 );
        block.set( expected );
        System.out.println( "Stored " +expected.length+" rel-node blocks " + block );
        
        // WHEN
        long[][] read = new long[expected.length][2];
        block.get( read );
        
        for ( int i = 0; i < read.length; i++ )
        {
            assertArrayEquals( "At index " + i, expected[i], read[i] );
        }
    }

    @Test
    public void testFindNodeForRelId() throws Exception {
        LinkBlock block = new LinkBlock( BufferType.SMALL );
        long[][] expected = idPairs( 10, 5, 50 );
        block.set( expected );
        assertEquals(-1, block.getNodeIdForRelId(0));
        assertEquals(-1, block.getNodeIdForRelId(9));
        assertEquals(-1, block.getNodeIdForRelId(60));
        assertEquals(5, block.getNodeIdForRelId(10));
        assertEquals(6, block.getNodeIdForRelId(11));
        assertEquals(54, block.getNodeIdForRelId(59));
    }

    private long[][] idPairs( long firstRelId, long firstNodeId, int count )
    {
        long result[][] = new long[count][2];
        for ( int i = 0; i < count; i++ )
        {
            result[i][0] = firstRelId+i;
            result[i][1] = firstNodeId+i;
        }
        return result;
    }
}
