import Redis from 'ioredis';

// Redis client singleton
let redisClient: Redis | null = null;
let isConnected = false;

// Fallback in-memory cache when Redis is unavailable
const memoryCache: Map<string, { data: string; expiry: number }> = new Map();

export async function initRedis(): Promise<boolean> {
  const redisUrl = process.env.REDIS_URL;
  
  if (!redisUrl) {
    console.log('⚠️ REDIS_URL not set, using in-memory cache fallback');
    return false;
  }

  try {
    redisClient = new Redis(redisUrl, {
      connectTimeout: 5000,
      maxRetriesPerRequest: 3,
      retryStrategy: (times: number) => {
        if (times > 10) {
          console.error('❌ Redis max retries reached, falling back to memory cache');
          return null;
        }
        return Math.min(times * 100, 3000);
      }
    });

    redisClient.on('error', (err: Error) => {
      console.error('❌ Redis error:', err.message);
      isConnected = false;
    });

    redisClient.on('connect', () => {
      console.log('🔗 Redis connecting...');
    });

    redisClient.on('ready', () => {
      console.log('✅ Redis connected and ready');
      isConnected = true;
    });

    redisClient.on('close', () => {
      console.log('🔌 Redis connection closed');
      isConnected = false;
    });

    // Test connection
    await redisClient.ping();
    isConnected = true;
    return true;
  } catch (error) {
    console.error('❌ Failed to connect to Redis:', error);
    isConnected = false;
    return false;
  }
}

export function isRedisConnected(): boolean {
  return isConnected && redisClient !== null;
}

export async function getCache<T>(key: string): Promise<T | null> {
  try {
    if (isConnected && redisClient) {
      const data = await redisClient.get(key);
      if (data) {
        return JSON.parse(data) as T;
      }
      return null;
    }
    
    // Fallback to memory cache
    const cached = memoryCache.get(key);
    if (cached && cached.expiry > Date.now()) {
      return JSON.parse(cached.data) as T;
    }
    return null;
  } catch (error) {
    console.error('Cache get error:', error);
    return null;
  }
}

export async function setCache(key: string, data: unknown, ttlSeconds: number = 60): Promise<void> {
  try {
    const serialized = JSON.stringify(data);
    
    if (isConnected && redisClient) {
      await redisClient.setex(key, ttlSeconds, serialized);
    } else {
      // Fallback to memory cache
      memoryCache.set(key, { 
        data: serialized, 
        expiry: Date.now() + ttlSeconds * 1000 
      });
    }
  } catch (error) {
    console.error('Cache set error:', error);
    // Still try memory cache on error
    memoryCache.set(key, { 
      data: JSON.stringify(data), 
      expiry: Date.now() + ttlSeconds * 1000 
    });
  }
}

export async function deleteCache(key: string): Promise<void> {
  try {
    if (isConnected && redisClient) {
      await redisClient.del(key);
    }
    memoryCache.delete(key);
  } catch (error) {
    console.error('Cache delete error:', error);
  }
}

// ---------------------------------------------------------------------------
// Stale-while-revalidate cache
//
// Problem this solves: endpoints with long TTLs (e.g. the all-time
// analytics charts, TTL 3600) rebuild synchronously on the first request
// after expiry. The rebuild fans out to BULK (klines × every symbol) and
// can take 10-20s — so once per hour, one unlucky visitor eats a 20s page
// load. Classic thundering-herd-of-one.
//
// With SWR the data is stored in an envelope { v, freshUntil } whose
// Redis TTL is much LONGER than the freshness window. On read:
//   - fresh hit  → serve
//   - stale hit  → serve the stale copy IMMEDIATELY, kick off a
//                  background rebuild (deduped across concurrent
//                  requests) that refreshes the envelope
//   - cold miss  → compute synchronously (first request after deploy
//                  or Redis flush), deduped so concurrent cold requests
//                  share one rebuild instead of stampeding BULK
// ---------------------------------------------------------------------------
type SwrEnvelope<T> = { v: T; freshUntil: number };

const inflightRebuilds = new Map<string, Promise<unknown>>();

export async function swrCache<T>(
  key: string,
  freshSeconds: number,
  rebuild: () => Promise<T>,
  // Keep the stale copy around well past freshness so there's always
  // something to serve while rebuilding. 24× covers a full day of
  // serving stale all-time data even if BULK is down for hours.
  hardTtlSeconds: number = freshSeconds * 24,
): Promise<T> {
  const raw = await getCache<SwrEnvelope<T>>(key);
  const now = Date.now();
  // Shape guard: entries written by the old direct setCache path (or any
  // other writer) aren't envelopes. Treat them as a cold miss so they get
  // rebuilt and rewritten in envelope form, rather than serving
  // `envelope.v === undefined` as a response body.
  const envelope =
    raw && typeof raw === 'object' && 'v' in raw && typeof (raw as SwrEnvelope<T>).freshUntil === 'number'
      ? raw
      : null;

  // Fresh hit — the fast path that should serve ~all requests.
  if (envelope && envelope.freshUntil > now) {
    return envelope.v;
  }

  const startRebuild = (): Promise<T> => {
    const existing = inflightRebuilds.get(key);
    if (existing) return existing as Promise<T>;
    const p = rebuild()
      .then(async (v) => {
        await setCache(
          key,
          { v, freshUntil: Date.now() + freshSeconds * 1000 } satisfies SwrEnvelope<T>,
          hardTtlSeconds,
        );
        return v;
      })
      .finally(() => {
        inflightRebuilds.delete(key);
      });
    inflightRebuilds.set(key, p);
    return p;
  };

  // Stale hit — serve immediately, refresh in background. Swallow
  // background failures: the stale copy stays valid in Redis and the
  // next request will retry the rebuild.
  if (envelope) {
    startRebuild().catch((e) =>
      console.error(`SWR background rebuild failed for ${key}:`, e),
    );
    return envelope.v;
  }

  // Cold miss — must wait, but concurrent cold requests share one rebuild.
  return startRebuild();
}

export async function deleteCachePattern(pattern: string): Promise<void> {
  try {
    if (isConnected && redisClient) {
      const keys = await redisClient.keys(pattern);
      if (keys.length > 0) {
        await redisClient.del(...keys);
      }
    }
    
    // Also clear from memory cache
    for (const key of memoryCache.keys()) {
      if (key.match(pattern.replace('*', '.*'))) {
        memoryCache.delete(key);
      }
    }
  } catch (error) {
    console.error('Cache pattern delete error:', error);
  }
}

// Get cache stats for monitoring
export async function getCacheStats(): Promise<{
  type: 'redis' | 'memory';
  connected: boolean;
  memoryKeys: number;
  redisInfo?: { dbSize: number };
}> {
  const stats: {
    type: 'redis' | 'memory';
    connected: boolean;
    memoryKeys: number;
    redisInfo?: { dbSize: number };
  } = {
    type: isConnected ? 'redis' : 'memory',
    connected: isConnected,
    memoryKeys: memoryCache.size,
  };

  if (isConnected && redisClient) {
    try {
      const dbSize = await redisClient.dbsize();
      stats.redisInfo = { dbSize };
    } catch (e) {
      // ignore
    }
  }

  return stats;
}

// Cleanup memory cache periodically (only matters when Redis is down)
setInterval(() => {
  const now = Date.now();
  for (const [key, value] of memoryCache.entries()) {
    if (value.expiry < now) {
      memoryCache.delete(key);
    }
  }
}, 60000); // Clean every minute

export { redisClient };
