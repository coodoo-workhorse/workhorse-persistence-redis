package io.coodoo.workhorse.persistence.redis.control.subscribe;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.ObservesAsync;
import javax.inject.Inject;

import io.coodoo.workhorse.persistence.redis.control.JedisExecution;
import io.coodoo.workhorse.persistence.redis.control.JedisOperation;
import redis.clients.jedis.Jedis;

/**
 * Class to subscribe a channel
 * 
 * @author coodoo GmbH (coodoo.io)
 */
@ApplicationScoped
public class RedisSubscriber {

    @Inject
    JedisExecution jedisExecution;

    @Inject
    RedisPubSub redisPubSub;

    public void psubscribe(@ObservesAsync ChannelToSubscribe channelToSubscribe) {

        jedisExecution.execute(new JedisOperation<Boolean>() {

            @SuppressWarnings("unchecked")
            @Override
            public Boolean perform(Jedis jedis) {

                jedis.psubscribe(redisPubSub, channelToSubscribe.channelId);
                return true;
            }
        });
    }
}
