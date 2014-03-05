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

import java.util.Arrays;
import java.util.Comparator;

public class LinkBlock
{
    private int idCount;
    private long lastRelId;
    private CompressedIdBuffer relIdDeltaBuffer;
    private CompressedIdBuffer nodeIdDeltaBuffer;

    public LinkBlock( BufferType type )
    {
        this.relIdDeltaBuffer = new CompressedIdBuffer( type, true );
        this.nodeIdDeltaBuffer = new CompressedIdBuffer( type, false );
    }

    public void set( long[][] relAndNodeIdPairs )
    {
        Arrays.sort( relAndNodeIdPairs, SORTER );

        lastRelId = relAndNodeIdPairs[relAndNodeIdPairs.length - 1][0];
        idCount = relAndNodeIdPairs.length;

        relIdDeltaBuffer.toggleMode();
        nodeIdDeltaBuffer.toggleMode();

        // Header
        relIdDeltaBuffer.store( lastRelId );
        relIdDeltaBuffer.store( idCount );

        // Ids
        long previousRelId = relAndNodeIdPairs[0][0];
        long previousRelNodeDelta = relAndNodeIdPairs[0][1] - previousRelId;
        relIdDeltaBuffer.store( previousRelId );
        nodeIdDeltaBuffer.store( previousRelNodeDelta );
        for ( int i = 1; i < relAndNodeIdPairs.length; i++ )
        {
            long[] pair = relAndNodeIdPairs[i];
            long relDelta = pair[0] - previousRelId;
            long relNodeDelta = pair[1] - pair[0];
            long derivativeRelNodeDelta = relNodeDelta - previousRelNodeDelta;
            relIdDeltaBuffer.store( relDelta );
            nodeIdDeltaBuffer.store( derivativeRelNodeDelta );
            previousRelId = pair[0];
            previousRelNodeDelta = relNodeDelta;
        }
        relIdDeltaBuffer.flush();
        nodeIdDeltaBuffer.flush();
    }

    public void get( long[][] target )
    {
        assert target.length >= idCount;

        relIdDeltaBuffer.toggleMode();
        nodeIdDeltaBuffer.toggleMode();
        // Header
        relIdDeltaBuffer.read();
        relIdDeltaBuffer.read();

        target[0][0] = relIdDeltaBuffer.read();
        target[0][1] = target[0][0] + nodeIdDeltaBuffer.read();
        for ( int i = 1; i < target.length; i++ )
        {
            long relDelta = relIdDeltaBuffer.read();
            long relId = target[i - 1][0] + relDelta;
            long derivativeRelNodeDelta = nodeIdDeltaBuffer.read();
            long previousRelNodeDelta = target[i - 1][1] - target[i - 1][0];
            target[i][0] = relId;
            target[i][1] = previousRelNodeDelta + relId + derivativeRelNodeDelta;
        }
    }

    public long getNodeIdForRelId( long targetReldId )
    {
        if ( targetReldId > lastRelId )
        {
            return -1;
        }

        relIdDeltaBuffer.toggleMode();
        nodeIdDeltaBuffer.toggleMode();

        // skip header
        relIdDeltaBuffer.read();
        relIdDeltaBuffer.read();

        long prevRelId = relIdDeltaBuffer.read();
        long prevNodeId = prevRelId + nodeIdDeltaBuffer.read();
        if ( targetReldId == prevRelId )
        {
            return prevNodeId;
        }
        for ( int i = 1; i < idCount; i++ )
        {
            long relDelta = relIdDeltaBuffer.read();
            long relId = prevRelId + relDelta;
            long derivativeRelNodeDelta = nodeIdDeltaBuffer.read();
            long previousRelNodeDelta = prevNodeId - prevRelId;
            prevRelId = relId;
            prevNodeId = previousRelNodeDelta + relId + derivativeRelNodeDelta;
            if ( targetReldId == prevRelId )
            {
                return prevNodeId;
            }
        }
        return -1; // todo throw not found exception?
    }


    private static final Comparator<long[]> SORTER = new Comparator<long[]>()
    {
        @Override
        public int compare( long[] o1, long[] o2 )
        {
            return Long.compare( o1[0], o2[0] );
        }
    };

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{ RelIdDeltas: " + relIdDeltaBuffer + " , NodeIdDeltas: " + nodeIdDeltaBuffer + "}";
    }
}
