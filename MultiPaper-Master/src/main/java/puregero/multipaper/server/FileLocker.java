package puregero.multipaper.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Put any files being written into a hashmap, and if they're read while they're
 * being written, return the bytes that are being written instead of reading
 * from the file.
 */
public class FileLocker {

    private static final Map<File, byte[]> beingWritten = new ConcurrentHashMap<>();
    private static final Map<File, CompletableFuture<Void>> locks = new ConcurrentHashMap<>();
    private static final Lock locksLock = new ReentrantLock();
    
    // Simple LRU cache for frequently accessed files
    private static final Map<File, CacheEntry> fileCache = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<File, CacheEntry> eldest) {
            return size() > MAX_CACHE_SIZE;
        }
    };
    private static final Lock cacheLock = new ReentrantLock();
    private static final int MAX_CACHE_SIZE = Integer.getInteger("filelocker.max.cache.size", 100);
    private static final long CACHE_EXPIRY_MS = Long.getLong("filelocker.cache.expiry.ms", 30000); // 30 seconds
    
    // Cache entry class to store file data and metadata
    private static class CacheEntry {
        final byte[] data;
        final long timestamp;
        
        CacheEntry(byte[] data) {
            this.data = data;
            this.timestamp = System.currentTimeMillis();
        }
        
        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > CACHE_EXPIRY_MS;
        }
    }

    public static CompletableFuture<CompletableFuture<Void>> createLockAsync(File file) {
        locksLock.lock();
        try {
            if (locks.containsKey(file)) {
                CompletableFuture<Void> existingLock = locks.get(file);
                return existingLock.thenCompose(value -> createLockAsync(file));
            }

            CompletableFuture<Void> lock = new CompletableFuture<>();
            
            // Use thenRun to automatically remove the lock when it completes
            locks.put(file, lock.thenRun(() -> locks.remove(file)));

            return CompletableFuture.completedFuture(lock);
        } finally {
            locksLock.unlock();
        }
    }

    public static byte[] readBytes(File file) throws IOException {
        // First check if the file is currently being written
        byte[] writingData = beingWritten.get(file);
        if (writingData != null) {
            return writingData;
        }
        
        // Then check the cache
        cacheLock.lock();
        try {
            CacheEntry entry = fileCache.get(file);
            if (entry != null) {
                if (!entry.isExpired()) {
                    return entry.data;
                } else {
                    // Remove expired entry
                    fileCache.remove(file);
                }
            }
        } finally {
            cacheLock.unlock();
        }
        
        // If not in cache, read from disk and cache the result
        if (!file.isFile()) {
            return new byte[0];
        }
        
        byte[] data = Files.readAllBytes(file.toPath());
        
        // Cache the result
        cacheLock.lock();
        try {
            fileCache.put(file, new CacheEntry(data));
        } finally {
            cacheLock.unlock();
        }
        
        return data;
    }

    public static void writeBytes(File file, byte[] bytes) throws IOException {
        // Use putIfAbsent to atomically check and put
        byte[] existing = beingWritten.putIfAbsent(file, bytes);
        if (existing != null) {
            // Another thread is writing to this file, wait for it to complete
            while (beingWritten.containsKey(file)) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for file write", e);
                }
            }
            // Try again
            writeBytes(file, bytes);
            return;
        }

        try {
            // Ensure the parent directory exists
            Path parentPath = file.toPath().getParent();
            if (parentPath != null) {
                Files.createDirectories(parentPath);
            }

            safeWrite(file, bytes);
            
            // Invalidate cache
            cacheLock.lock();
            try {
                fileCache.remove(file);
            } finally {
                cacheLock.unlock();
            }
        } finally {
            beingWritten.remove(file);
        }
    }

    private static void safeWrite(File file, byte[] bytes) throws IOException {
        File newFile = new File(file.getParentFile(), file.getName() + "_new");
        File oldFile = new File(file.getParentFile(), file.getName() + "_old");

        Files.write(newFile.toPath(), bytes);
        safeReplaceFile(file.toPath(), newFile.toPath(), oldFile.toPath());
    }

    private static void safeReplaceFile(Path file, Path newFile, Path oldFile) throws IOException {
        if (Files.exists(file)) {
            Files.move(file, oldFile, StandardCopyOption.REPLACE_EXISTING);
        }

        Files.move(newFile, file, StandardCopyOption.REPLACE_EXISTING);

        if (Files.exists(oldFile)) {
            Files.delete(oldFile);
        }
    }
}