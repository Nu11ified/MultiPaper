package puregero.multipaper.server.util;

/*
** 2011 January 5
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
*/

/*
* 2011 February 16
*
* This source code is based on the work of Scaevolus (see notice above).
* It has been slightly modified by Mojang AB to limit the maximum cache
* size (relevant to extremely big worlds on Linux systems with limited
* number of file handles). The region files are postfixed with ".mcr"
* (Minecraft region file) instead of ".data" to differentiate from the
* original McRegion files.
*
*/

// A simple cache and wrapper for efficiently multiple RegionFiles simultaneously.

import java.io.*;
import java.lang.ref.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RegionFileCache {

    private static final int MAX_CACHE_SIZE = Integer.getInteger("max.regionfile.cache.size", 256);
    
    // Use a ReadWriteLock for better concurrency - many reads can happen simultaneously
    private static final ReadWriteLock cacheLock = new ReentrantReadWriteLock();
    private static final Lock readLock = cacheLock.readLock();
    private static final Lock writeLock = cacheLock.writeLock();

    // Use a ConcurrentHashMap for the canonical path cache to avoid locking
    private static final Map<File, File> canonicalPathCache = new ConcurrentHashMap<>();
    
    // Use a LinkedHashMap with access order for LRU behavior
    private static final Map<File, Reference<RegionFile>> cache = new LinkedHashMap<>(16, 0.75f, true);

    private RegionFileCache() {
    }

    public static boolean isRegionFileOpen(File regionDir, int chunkX, int chunkZ) {
        File file = getFileForRegionFile(regionDir, chunkX, chunkZ);
        file = canonical(file);

        readLock.lock();
        try {
            Reference<RegionFile> ref = cache.get(file);
            return ref != null && ref.get() != null;
        } finally {
            readLock.unlock();
        }
    }

    private static File canonical(File file) {
        // First check the cache
        File cachedPath = canonicalPathCache.get(file);
        if (cachedPath != null) {
            return cachedPath;
        }
        
        try {
            // Remove any .'s and ..'s
            File canonicalFile = new File(file.getCanonicalPath());
            canonicalPathCache.put(file, canonicalFile);
            return canonicalFile;
        } catch (IOException e) {
            e.printStackTrace();
            return file;
        }
    }
    
    private static File getFileForRegionFile(File regionDir, int chunkX, int chunkZ) {
        return new File(regionDir, "r." + (chunkX >> 5) + "." + (chunkZ >> 5) + ".mca");
    }

    public static RegionFile getRegionFileIfExists(File regionDir, int chunkX, int chunkZ) {
        File file = getFileForRegionFile(regionDir, chunkX, chunkZ);
        file = canonical(file);

        // First try with read lock
        readLock.lock();
        try {
            Reference<RegionFile> ref = cache.get(file);
            if (ref != null) {
                RegionFile regionFile = ref.get();
                if (regionFile != null) {
                    return regionFile;
                }
                // Reference was cleared by GC, remove it from cache
                writeLock.lock();
                try {
                    cache.remove(file);
                } finally {
                    writeLock.unlock();
                }
            }
        } finally {
            readLock.unlock();
        }

        // Check if file exists without locking
        if (!Files.exists(file.toPath())) {
            return null;
        }
        
        // File exists but not in cache, get it with write lock
        return getRegionFile(regionDir, chunkX, chunkZ);
    }

    public static RegionFile getRegionFile(File regionDir, int chunkX, int chunkZ) {
        File file = getFileForRegionFile(regionDir, chunkX, chunkZ);
        file = canonical(file);

        // First try with read lock
        readLock.lock();
        try {
            Reference<RegionFile> ref = cache.get(file);
            if (ref != null) {
                RegionFile regionFile = ref.get();
                if (regionFile != null) {
                    return regionFile;
                }
            }
        } finally {
            readLock.unlock();
        }

        // Not in cache or reference was cleared, acquire write lock
        writeLock.lock();
        try {
            // Check again in case another thread added it while we were waiting
            Reference<RegionFile> ref = cache.get(file);
            if (ref != null) {
                RegionFile regionFile = ref.get();
                if (regionFile != null) {
                    return regionFile;
                }
                // Reference was cleared by GC, remove it from cache
                cache.remove(file);
            }

            // Ensure directory exists
            if (!regionDir.exists()) {
                regionDir.mkdirs();
            }

            // Check cache size and clear if needed
            if (cache.size() >= MAX_CACHE_SIZE) {
                clearOne();
            }

            // Create new RegionFile and add to cache
            RegionFile reg = new RegionFile(file);
            cache.put(file, new SoftReference<>(reg));
            return reg;
        } finally {
            writeLock.unlock();
        }
    }

    private static void clearOne() {
        // Must be called with writeLock held
        Iterator<Map.Entry<File, Reference<RegionFile>>> it = cache.entrySet().iterator();
        if (it.hasNext()) {
            Map.Entry<File, Reference<RegionFile>> clearEntry = it.next();
            it.remove(); // Use iterator.remove() for better performance
            
            try {
                RegionFile removeFile = clearEntry.getValue().get();
                if (removeFile != null) {
                    removeFile.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static int getSizeDelta(File basePath, int chunkX, int chunkZ) {
        RegionFile r = getRegionFile(basePath, chunkX, chunkZ);
        return r.getSizeDelta();
    }

    public static DataInputStream getChunkDataInputStream(File basePath, int chunkX, int chunkZ) {
        RegionFile r = getRegionFile(basePath, chunkX, chunkZ);
        if (r != null) {
            return r.getChunkDataInputStream(chunkX, chunkZ);
        } else {
            return null;
        }
    }

    public static DataOutputStream getChunkDataOutputStream(File basePath, int chunkX, int chunkZ) {
        RegionFile r = getRegionFile(basePath, chunkX, chunkZ);
        return r.getChunkDataOutputStream(chunkX, chunkZ);
    }

    public static CompletableFuture<byte[]> getChunkDeflatedDataAsync(File basePath, int chunkX, int chunkZ) {
        RegionFile r = getRegionFileIfExists(basePath, chunkX, chunkZ);
        if (r != null) {
            return r.submitTask(regionFile -> regionFile.getDeflatedBytes(chunkX, chunkZ));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static byte[] getChunkDeflatedData(File basePath, int chunkX, int chunkZ) {
        try {
            RegionFile r = getRegionFileIfExists(basePath, chunkX, chunkZ);
            if (r != null) {
                return r.getDeflatedBytes(chunkX, chunkZ);
            } else {
                return null;
            }
        } catch (Throwable throwable) {
            System.err.println("Error when trying to read chunk " + chunkX + "," + chunkZ + " in " + basePath);
            throw throwable;
        }
    }

    public static CompletableFuture<Void> putChunkDeflatedDataAsync(File basePath, int chunkX, int chunkZ, byte[] data) {
        RegionFile r = getRegionFile(basePath, chunkX, chunkZ);
        return r.submitTask(regionFile -> {
            regionFile.putDeflatedBytes(chunkX, chunkZ, data);
            return null;
        });
    }

    private static void putChunkDeflatedData(File basePath, int chunkX, int chunkZ, byte[] data) {
        try {
            RegionFile r = getRegionFile(basePath, chunkX, chunkZ);
            r.putDeflatedBytes(chunkX, chunkZ, data);
        } catch (Throwable throwable) {
            System.err.println("Error when trying to write chunk " + chunkX + "," + chunkZ + " in " + basePath);
            throw throwable;
        }
    }
}
