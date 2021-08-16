package io.coodoo.workhorse.persistence.redis.entity;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.coodoo.workhorse.core.entity.WorkhorseConfig;
import io.coodoo.workhorse.persistence.interfaces.ConfigPersistence;
import io.coodoo.workhorse.persistence.redis.boundary.RedisPersistenceConfig;
import io.coodoo.workhorse.persistence.redis.control.JedisExecution;
import io.coodoo.workhorse.persistence.redis.control.RedisController;
import io.coodoo.workhorse.persistence.redis.control.RedisKey;

@ApplicationScoped
public class RedisConfigPersistence implements ConfigPersistence {

    @Inject
    JedisExecution jedisExecution;

    @Inject
    RedisController redisService;

    @Override
    public WorkhorseConfig get() {

        return redisService.get(RedisKey.JOB_ENGINE_CONFIG.getQuery(), RedisPersistenceConfig.class);
    }

    @Override
    public WorkhorseConfig update(WorkhorseConfig workhorseConfig) {
        redisService.set(RedisKey.JOB_ENGINE_CONFIG.getQuery(), (RedisPersistenceConfig) workhorseConfig);
        return get();
    }

    @Override
    public String getPersistenceName() {

        return RedisPersistenceConfig.NAME;
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

        RedisPersistenceConfig redisPersistenceConfig = (RedisPersistenceConfig) params[0];

        RedisPersistenceConfig.redisHost = redisPersistenceConfig.getRedisHost();
        RedisPersistenceConfig.redisPort = redisPersistenceConfig.getRedisPort();

        jedisExecution.init();

    }

}
