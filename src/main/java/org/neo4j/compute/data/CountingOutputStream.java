package org.neo4j.compute.data;

import java.io.IOException;
import java.io.OutputStream;

/**
* @author mh
* @since 07.12.13
*/
public class CountingOutputStream extends OutputStream {
    int count = 0;

    public void write(int b) throws IOException {
        count++;
    }

    public int getCount() {
        return count;
    }
}
