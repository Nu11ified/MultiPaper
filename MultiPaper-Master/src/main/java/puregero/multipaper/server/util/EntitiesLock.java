package puregero.multipaper.server.util;

import puregero.multipaper.mastermessagingprotocol.ChunkKey;

public class EntitiesLock {

    /**
     * The number of locks to use. Must be a power of two.
     * Can be configured with the system property 'puregero.multipaper.entitieslock.count'.
     */
    private static final int LOCK_COUNT = Integer.getInteger("puregero.multipaper.entitieslock.count", 64);
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

    public static Object getEntitiesLock(ChunkKey key) {
        return locks[key.hashCode() & (LOCK_COUNT - 1)];
    }

}