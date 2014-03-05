package org.neo4j.helpers.idcompression;

import java.nio.ByteBuffer;

import static java.lang.String.format;

public class CompressedIdBuffer
{
    private final static long MARKER = Long.MAX_VALUE;

    private final ByteBuffer buffer;
    private final LongEncoder encoder;
    private long lastValue;
    private int counter;
    private int stored;
    private int bytes;

    public CompressedIdBuffer( ByteBuffer buffer, boolean unsigned )
    {
        this.buffer = buffer;
        this.encoder = unsigned ? new UnsignedLongBase128Encoder() : new SignedLongBase128Encoder();
    }

    public CompressedIdBuffer( BufferType type, boolean unsigned )
    {
        this( type.allocateBuffer(), unsigned );
    }

    public void toggleMode()
    {
        flush();
        buffer.position( BufferType.HEADER );
        lastValue = 0;
        counter = 0;
    }

    public void store( long value )
    {
        if ( value == lastValue )
        {
            counter++;
        }
        else if ( counter == 0 )
        {
            counter = 1;
            lastValue = value;
        }
        else
        {
            if ( counter > 1 )
            {
                bytes += encode( MARKER );
                bytes += encode( counter );
            }
            bytes += encode( lastValue );
            stored += counter;
            counter = 1;
            lastValue = value;
        }
    }

    public long read()
    {
        if ( counter == 0 )
        {
            long value = decode();
            if ( value == MARKER )
            {
                counter = (int) decode();
                lastValue = decode();
            }
            else
            {
                counter = 1;
                lastValue = value;
            }
        }
        counter--;
        return lastValue;
    }

    private int encode( long value )
    {
        if ( value == MARKER )
        {
            return encoder.encode( buffer, 0 );
        }

        if ( value >= 0 )
        {
            return encoder.encode( buffer, value + 1 );
        }

        return encoder.encode( buffer, value - 1 );
    }

    private long decode()
    {
        long result = encoder.decode( buffer );
        if ( result == 0 )
        {
            return MARKER;
        }
        if ( result >= 0 )
        {
            return result - 1;
        }
        return result + 1;
    }

    public void flush()
    {
        if ( counter > 0 )
        {
            store( lastValue + 1 ); // something that differs from the last value
        }
    }

    @Override
    public String toString()
    {
        return format( "CompressedIdBuffer stored ids %d bytes used %d ByteBuffer size: %d bytes/id %.10f",
                stored, bytes, buffer.capacity(), getBytesPerValue() );
    }

    int getStored()
    {
        return stored;
    }

    public float getBytesPerValue()
    {
        return (float)bytes / stored;
    }
}
