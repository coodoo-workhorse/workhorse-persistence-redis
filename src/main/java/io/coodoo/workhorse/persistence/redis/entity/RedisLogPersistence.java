package io.coodoo.workhorse.persistence.redis.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.coodoo.workhorse.core.entity.WorkhorseLog;
import io.coodoo.workhorse.persistence.interfaces.LogPersistence;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingParameters;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingResult;
import io.coodoo.workhorse.persistence.interfaces.listing.Metadata;
import io.coodoo.workhorse.persistence.redis.boundary.RedisPersistenceConfig;
import io.coodoo.workhorse.persistence.redis.control.JedisExecution;
import io.coodoo.workhorse.persistence.redis.control.JedisOperation;
import io.coodoo.workhorse.persistence.redis.control.RedisController;
import io.coodoo.workhorse.persistence.redis.control.RedisKey;
import io.coodoo.workhorse.util.WorkhorseUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

@ApplicationScoped
public class RedisLogPersistence implements LogPersistence {

    @Inject
    RedisController redisService;

    @Inject
    JedisExecution jedisExecution;

    @Override
    public WorkhorseLog get(Long logId) {

        String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(logId);
        return redisService.get(workhorseLogKey, WorkhorseLog.class);
    }

    @Override
    public WorkhorseLog update(Long workhorseLogId, WorkhorseLog workhorseLog) {

        String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);
        redisService.set(workhorseLogKey, workhorseLog);

        return workhorseLog;
    }

    @Override
    public WorkhorseLog delete(Long logId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WorkhorseLog persist(WorkhorseLog workhorseLog) {

        Long workhorseLogId = redisService.incr(RedisKey.INC_WORKHORSE_LOG_ID.getQuery());
        workhorseLog.setId(workhorseLogId);
        workhorseLog.setCreatedAt(WorkhorseUtil.timestamp());

        String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);
        redisService.set(workhorseLogKey, workhorseLog);

        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();
        redisService.rpush(workhorseLogListKey, workhorseLogId);

        Long jobId = workhorseLog.getJobId();
        if (jobId != null) {
            String workhorseLogByJobListKey = RedisKey.LIST_OF_WORKHORSE_LOG_BY_JOB.getQuery(jobId);
            redisService.rpush(workhorseLogByJobListKey, workhorseLog);
        }

        return workhorseLog;
    }

    @Override
    public List<WorkhorseLog> getAll(int limit) {
        String workhorseLogListKey = RedisKey.WORKHORSE_LOG_LIST.getQuery();
        List<Long> workhorseLogIds = redisService.lrange(workhorseLogListKey, Long.class, 0, limit);

        List<String> workhorseLogKeys = new ArrayList<>();
        for (Long workhorseLogId : workhorseLogIds) {
            String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);
            workhorseLogKeys.add(workhorseLogKey);
        }

        List<WorkhorseLog> workhorseLogs = redisService.get(workhorseLogKeys, WorkhorseLog.class);

        return workhorseLogs;
    }

    @Override
    public ListingResult<WorkhorseLog> getWorkhorseLogListing(ListingParameters listingParameters) {

        List<WorkhorseLog> result = getAll(listingParameters.getLimit());
        Metadata metadata = new Metadata(Long.valueOf(result.size()), listingParameters);

        return new ListingResult<WorkhorseLog>(result, metadata);
    }

    @Override
    public int deleteByJobId(Long jobId) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getPersistenceName() {
        return RedisPersistenceConfig.NAME;
    }

    @Override
    public void connect(Object... params) {
        // TODO Auto-generated method stub

    }

    private Map<Long, Response<String>> getAllWorkhorseLogById(List<Long> workhorseLogIds) {

        Map<Long, Response<String>> responseMap = new HashMap<>();

        jedisExecution.execute(new JedisOperation<Long>() {

            @SuppressWarnings("unchecked")
            @Override
            public Long perform(Jedis jedis) {
                long length = 0;
                Pipeline pipeline = jedis.pipelined();

                for (Long workhorseLogId : workhorseLogIds) {
                    String workhorseLogKey = RedisKey.WORKHORSE_LOG_BY_ID.getQuery(workhorseLogId);

                    Response<String> foundWorkhorseLog = pipeline.get(workhorseLogKey);
                    responseMap.put(workhorseLogId, foundWorkhorseLog);
                    length++;
                }

                pipeline.sync();
                return length;
            }

        });

        return responseMap;

    }

}
