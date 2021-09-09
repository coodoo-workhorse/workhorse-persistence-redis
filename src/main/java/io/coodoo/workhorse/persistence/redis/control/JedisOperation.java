package io.coodoo.workhorse.persistence.redis.control;

import redis.clients.jedis.Jedis;

/**
 * @author coodoo GmbH (coodoo.io)
 */
public interface JedisOperation<T> {

    @SuppressWarnings("hiding")
    <T> T perform(Jedis jedis);
}
