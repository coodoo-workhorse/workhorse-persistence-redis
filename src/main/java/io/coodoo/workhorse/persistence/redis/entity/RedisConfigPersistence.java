package io.coodoo.workhorse.persistence.redis.entity;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.coodoo.workhorse.core.entity.WorkhorseConfig;
import io.coodoo.workhorse.persistence.interfaces.ConfigPersistence;
import io.coodoo.workhorse.persistence.redis.boundary.RedisPersistenceConfig;
import io.coodoo.workhorse.persistence.redis.control.JedisExecution;

@ApplicationScoped
public class RedisConfigPersistence implements ConfigPersistence {

    @Inject
    JedisExecution jedisExecution;

    @Override
    public WorkhorseConfig get() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WorkhorseConfig update(WorkhorseConfig workhorseConfig) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getPersistenceName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getPersistenceVersion() {
        // TODO Auto-generated method stub
        return null;
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
