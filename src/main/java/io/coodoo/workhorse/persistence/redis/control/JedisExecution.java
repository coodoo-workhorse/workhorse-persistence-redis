package io.coodoo.workhorse.persistence.redis.control;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Destroyed;
import javax.enterprise.event.Observes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.coodoo.workhorse.persistence.redis.boundary.StaticRedisConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author coodoo GmbH (coodoo.io)
 */
@ApplicationScoped
public class JedisExecution {

    private static final Logger log = LoggerFactory.getLogger(JedisExecution.class);

    private JedisPool jedisPool;

    public void init() {

        log.info("Creating Redis-Pool mit HOST: {}:{}  ", StaticRedisConfig.REDIS_HOST, StaticRedisConfig.REDIS_PORT);

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(StaticRedisConfig.MAX_TOTAL);
        poolConfig.setMinIdle(StaticRedisConfig.MIN_IDLE);
        poolConfig.setMaxIdle(StaticRedisConfig.MAX_IDLE);

        jedisPool = new JedisPool(poolConfig, StaticRedisConfig.REDIS_HOST, StaticRedisConfig.REDIS_PORT, StaticRedisConfig.TIME_OUT,
                        StaticRedisConfig.REDIS_PASSWORD);

    }

    public <T> T execute(JedisOperation<T> operation) {

        if (jedisPool.getNumActive() > 500) {

            log.info("Redis-Pool Stats. Active: {}, Idle: {}, Waiters: {}", jedisPool.getNumActive(), jedisPool.getNumIdle(), jedisPool.getNumWaiters());
        }
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            return operation.perform(jedis);
        } catch (JedisConnectionException e) {
            // wieso schluckenb wir hier die exception? ich halte das für gefährlich, denn so enden wir mit korrupten datenständen?!
            log.error("JedisConnectionException BY execute.", e);
            if (null != jedis) {
                jedis.close();
                jedis = null;
            }
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }
        return null;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void destroy(@Observes @Destroyed(ApplicationScoped.class) Object init) {

        log.info("Close Redis-Pool / Connections: Active: {}, Idle: {}, Waiters: {}", jedisPool.getNumActive(), jedisPool.getNumIdle(),
                        jedisPool.getNumWaiters());

        jedisPool.destroy();
    }

}
