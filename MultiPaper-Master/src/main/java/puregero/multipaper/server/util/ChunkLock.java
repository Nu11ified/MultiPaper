package puregero.multipaper.server.util;

import puregero.multipaper.mastermessagingprotocol.ChunkKey;

public class ChunkLock {

    /**
     * The number of locks to use. Must be a power of two.
     * Can be configured with the system property 'puregero.multipaper.chunklock.count'.
     */
    private static final int LOCK_COUNT = Integer.getInteger("puregero.multipaper.chunklock.count", 64);
    private static final Object[] locks;

    static {
        // LOCK_COUNT must be a power of two for the bitwise AND trick to work as a fast modulo.
        if (LOCK_COUNT <= 0 || Integer.bitCount(LOCK_COUNT) != 1) {
            throw new IllegalStateException("Lock count must be a positive power of two, but is " + LOCK_COUNT);
        }

        locks = new Object[LOCK_COUNT];
        for (int i = 0; i < LOCK_COUNT; i++) {
            locks[i] = new Object();
        }
    }

    public static Object getChunkLock(ChunkKey key) {
        return locks[key.hashCode() & (LOCK_COUNT - 1)];
    }

}