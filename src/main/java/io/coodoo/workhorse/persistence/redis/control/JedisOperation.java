package io.coodoo.workhorse.persistence.redis.control;

import redis.clients.jedis.Jedis;

public interface JedisOperation<T> {

    <T> T perform(Jedis jedis);
}
