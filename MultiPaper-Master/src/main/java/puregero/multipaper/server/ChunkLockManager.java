package puregero.multipaper.server;

import puregero.multipaper.mastermessagingprotocol.ChunkKey;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ChunkLockManager {

    // Configurable timeout values
    private static final int DEFAULT_LOCK_TIMEOUT_SECONDS = Integer.getInteger("chunklockmanager.timeout.seconds", 30);
    private static final int CLEANUP_INTERVAL_SECONDS = Integer.getInteger("chunklockmanager.cleanup.interval.seconds", 60);
    
    // Use a counter to track lock statistics
    private static final AtomicInteger activeLocksCount = new AtomicInteger(0);
    private static final AtomicInteger timeoutsCount = new AtomicInteger(0);
    
    // Map to store chunk locks
    private static final Map<ChunkKey, ChunkLock> locks = new ConcurrentHashMap<>();
    
    // Scheduled executor for cleanup tasks
    private static final ScheduledExecutorService cleanupExecutor;
    
    static {
        // Initialize the cleanup executor with a single daemon thread
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r, "ChunkLockManager-Cleanup");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        cleanupExecutor = executor;
        
        // Schedule periodic cleanup of expired locks
        cleanupExecutor.scheduleAtFixedRate(ChunkLockManager::cleanupExpiredLocks, 
                CLEANUP_INTERVAL_SECONDS, CLEANUP_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }
    
    // Inner class to represent a chunk lock with creation timestamp
    private static class ChunkLock {
        final CompletableFuture<Void> future;
        final long creationTime;
        
        ChunkLock(int timeoutSeconds) {
            this.future = new CompletableFuture<>();
            this.creationTime = System.currentTimeMillis();
            
            // Set up timeout
            CompletableFuture.delayedExecutor(timeoutSeconds, TimeUnit.SECONDS).execute(() -> {
                if (!future.isDone()) {
                    future.complete(null);
                    timeoutsCount.incrementAndGet();
                }
            });
        }
        
        boolean isExpired() {
            return future.isDone() || 
                   System.currentTimeMillis() - creationTime > TimeUnit.SECONDS.toMillis(DEFAULT_LOCK_TIMEOUT_SECONDS * 2L);
        }
    }

    /**
     * Creates a lock for a chunk that will be automatically released after a timeout
     * or when writtenChunk is called.
     */
    public static void lockUntilWrite(String world, int cx, int cz) {
        ChunkKey key = new ChunkKey(world, cx, cz);
        ChunkLock lock = new ChunkLock(DEFAULT_LOCK_TIMEOUT_SECONDS);
        
        locks.put(key, lock);
        activeLocksCount.incrementAndGet();
    }

    /**
     * Releases a lock on a chunk, notifying any waiting operations.
     */
    public static void writtenChunk(String world, int cx, int cz) {
        ChunkKey key = new ChunkKey(world, cx, cz);
        ChunkLock lock = locks.remove(key);

        if (lock != null) {
            lock.future.complete(null);
            activeLocksCount.decrementAndGet();
        }
    }

    /**
     * Waits for a lock on a chunk to be released before executing the callback.
     */
    public static void waitForLock(String world, int cx, int cz, Runnable callback) {
        ChunkKey key = new ChunkKey(world, cx, cz);
        ChunkLock lock = locks.get(key);

        if (lock != null) {
            lock.future.thenRun(callback);
        } else {
            callback.run();
        }
    }
    
    /**
     * Cleans up expired locks to prevent memory leaks.
     */
    private static void cleanupExpiredLocks() {
        locks.entrySet().removeIf(entry -> {
            if (entry.getValue().isExpired()) {
                activeLocksCount.decrementAndGet();
                return true;
            }
            return false;
        });
    }
    
    /**
     * Returns the current number of active locks.
     */
    public static int getActiveLocksCount() {
        return activeLocksCount.get();
    }
    
    /**
     * Returns the number of locks that have timed out.
     */
    public static int getTimeoutsCount() {
        return timeoutsCount.get();
    }
}
