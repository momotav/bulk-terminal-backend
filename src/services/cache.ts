import { createClient, RedisClientType } from 'redis';

// Redis client singleton
let redisClient: RedisClientType | null = null;
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
    redisClient = createClient({
      url: redisUrl,
      socket: {
        connectTimeout: 5000,
        reconnectStrategy: (retries: number) => {
          if (retries > 10) {
            console.error('❌ Redis max retries reached, falling back to memory cache');
            return false;
          }
          return Math.min(retries * 100, 3000);
        }
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

    redisClient.on('end', () => {
      console.log('🔌 Redis connection closed');
      isConnected = false;
    });

    await redisClient.connect();
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

export async function setCache(key: string, data: any, ttlSeconds: number = 60): Promise<void> {
  try {
    const serialized = JSON.stringify(data);
    
    if (isConnected && redisClient) {
      await redisClient.setEx(key, ttlSeconds, serialized);
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

export async function deleteCachePattern(pattern: string): Promise<void> {
  try {
    if (isConnected && redisClient) {
      const keys = await redisClient.keys(pattern);
      if (keys.length > 0) {
        await redisClient.del(keys);
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
  redisInfo?: any;
}> {
  const stats = {
    type: (isConnected ? 'redis' : 'memory') as 'redis' | 'memory',
    connected: isConnected,
    memoryKeys: memoryCache.size,
    redisInfo: null as any
  };

  if (isConnected && redisClient) {
    try {
      const info = await redisClient.info('memory');
      const dbSize = await redisClient.dbSize();
      stats.redisInfo = {
        dbSize,
        memorySnippet: info.slice(0, 500)
      };
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
