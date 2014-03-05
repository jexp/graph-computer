package org.neo4j.helpers.idcompression;

import org.junit.Test;
import org.neo4j.kernel.impl.util.Bits;

import java.util.Random;

/**
 * @author mh
 * @since 15.12.13
 */
public class BitStorageTest {
    private final static int COUNT = 1024*1024*1024;
    public static final int RANDOM_BITS = 1024;

    // 3.5s for 1GB longs
    @Test
    public void testBitSet() throws Exception {
        int[] randomBits = randomBits();
        Bits bits = Bits.bits(COUNT);
        long time = System.currentTimeMillis();
        for (int i=0;i< COUNT;i++) {
            if ((i | randomBits[i % RANDOM_BITS]) != 0) {
                bits.put(1,1);
            }
        }
        long delta = System.currentTimeMillis() - time;
        System.out.println("delta = " + delta);
    }

    // ??s for 1GB longs
    @Test
    public void testCompressedBitSet() throws Exception {
        int[] randomBits = randomBits();
        CompressedBits bits = new CompressedBits();
        long time = System.currentTimeMillis();
        for (int i=0;i< COUNT;i++) {
            if ((i | randomBits[i % RANDOM_BITS]) != 0) {
                bits.set(i);
            }
        }
        long delta = System.currentTimeMillis() - time;
        System.out.println("delta = " + delta);
    }

    private int[] randomBits() {
        Random random = new Random();
        int[] randomBits = new int[RANDOM_BITS];
        for (int i = RANDOM_BITS - 1; i >= 0; i--) {
            randomBits[i]=random.nextInt();
        }
        return randomBits;
    }
}
