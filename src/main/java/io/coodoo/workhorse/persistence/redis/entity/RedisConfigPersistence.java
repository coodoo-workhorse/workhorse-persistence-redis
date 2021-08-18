package io.coodoo.workhorse.persistence.redis.entity;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.coodoo.workhorse.core.entity.WorkhorseConfig;
import io.coodoo.workhorse.persistence.interfaces.ConfigPersistence;
import io.coodoo.workhorse.persistence.redis.boundary.RedisPersistenceConfig;
import io.coodoo.workhorse.persistence.redis.boundary.StaticRedisConfig;
import io.coodoo.workhorse.persistence.redis.control.JedisExecution;
import io.coodoo.workhorse.persistence.redis.control.RedisKey;
import io.coodoo.workhorse.persistence.redis.control.RedisService;

/**
 * @author coodoo GmbH (coodoo.io)
 */
@ApplicationScoped
public class RedisConfigPersistence implements ConfigPersistence {

    @Inject
    JedisExecution jedisExecution;

    @Inject
    RedisService redisService;

    @Override
    public WorkhorseConfig get() {

        // RedisPersistenceConfig config = redisService.get(RedisKey.JOB_ENGINE_CONFIG.getQuery(), RedisPersistenceConfig.class);
        //
        // WorkhorseConfig workhorseConfig = new RedisPersistenceConfig();
        //
        // workhorseConfig.setTimeZone(config.getTimeZone());
        // workhorseConfig.setBufferMax(config.getBufferMax());
        // workhorseConfig.setBufferMin(config.getBufferMin());
        // workhorseConfig.setBufferPollInterval(config.getBufferPollInterval());
        // workhorseConfig.setExecutionTimeout(config.getExecutionTimeout());
        // workhorseConfig.setExecutionTimeoutStatus(config.getExecutionTimeoutStatus());
        // workhorseConfig.setBufferPushFallbackPollInterval(config.getBufferPushFallbackPollInterval());
        // workhorseConfig.setMinutesUntilCleanup(config.getMinutesUntilCleanup());
        // workhorseConfig.setMaxExecutionSummaryLength(config.getMaxExecutionSummaryLength());
        // workhorseConfig.setLogChange(config.getLogChange());
        // workhorseConfig.setLogTimeFormat(config.getLogTimeFormat());
        // workhorseConfig.setLogInfoMarker(config.getLogInfoMarker());
        // workhorseConfig.setLogWarnMarker(config.getLogWarnMarker());
        // workhorseConfig.setLogErrorMarker(config.getLogErrorMarker());
        // return workhorseConfig;

        return redisService.get(RedisKey.JOB_ENGINE_CONFIG.getQuery(), RedisPersistenceConfig.class);
    }

    @Override
    public WorkhorseConfig update(WorkhorseConfig workhorseConfig) {

        RedisPersistenceConfig redisPersisitenceConfig = new RedisPersistenceConfig();
        redisPersisitenceConfig.setTimeZone(workhorseConfig.getTimeZone());
        redisPersisitenceConfig.setBufferPollInterval(workhorseConfig.getBufferPollInterval());
        redisPersisitenceConfig.setBufferPushFallbackPollInterval(workhorseConfig.getBufferPushFallbackPollInterval());
        redisPersisitenceConfig.setBufferMax(workhorseConfig.getBufferMax());
        redisPersisitenceConfig.setBufferMin(workhorseConfig.getBufferMin());
        redisPersisitenceConfig.setMaxExecutionSummaryLength(workhorseConfig.getMaxExecutionSummaryLength());
        redisPersisitenceConfig.setExecutionTimeout(workhorseConfig.getExecutionTimeout());
        redisPersisitenceConfig.setExecutionTimeoutStatus(workhorseConfig.getExecutionTimeoutStatus());
        redisPersisitenceConfig.setMinutesUntilCleanup(workhorseConfig.getMinutesUntilCleanup());
        redisPersisitenceConfig.setLogChange(workhorseConfig.getLogChange());
        redisPersisitenceConfig.setLogTimeFormat(workhorseConfig.getLogTimeFormat());
        redisPersisitenceConfig.setLogInfoMarker(workhorseConfig.getLogInfoMarker());
        redisPersisitenceConfig.setLogWarnMarker(workhorseConfig.getLogWarnMarker());
        redisPersisitenceConfig.setLogErrorMarker(workhorseConfig.getLogErrorMarker());
        redisService.set(RedisKey.JOB_ENGINE_CONFIG.getQuery(), redisPersisitenceConfig);
        return get();
    }

    @Override
    public String getPersistenceName() {

        return StaticRedisConfig.NAME;
    }

    @Override
    public String getPersistenceVersion() {

        return new RedisPersistenceConfig().getPersistenceVersion();
    }

    @Override
    public void connect(Object... params) {

        if (params == null || params.length == 0) {
            return;
        }

        if (!(params[0] instanceof RedisPersistenceConfig)) {
            return;
        }

        RedisPersistenceConfig redisPersistenceConfig = (RedisPersistenceConfig) params[0];
        StaticRedisConfig.REDIS_HOST = redisPersistenceConfig.getRedisHost();
        StaticRedisConfig.REDIS_PORT = redisPersistenceConfig.getRedisPort();
        StaticRedisConfig.MAX_TOTAL = redisPersistenceConfig.getMaxTotal();
        StaticRedisConfig.MAX_IDLE = redisPersistenceConfig.getMaxIdle();
        StaticRedisConfig.MIN_IDLE = redisPersistenceConfig.getMinIdle();

        jedisExecution.init();

    }

}
