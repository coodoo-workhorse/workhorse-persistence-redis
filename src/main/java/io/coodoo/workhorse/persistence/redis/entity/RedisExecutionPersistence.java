package io.coodoo.workhorse.persistence.redis.entity;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.coodoo.workhorse.core.entity.Execution;
import io.coodoo.workhorse.core.entity.ExecutionFailStatus;
import io.coodoo.workhorse.core.entity.ExecutionLog;
import io.coodoo.workhorse.core.entity.ExecutionStatus;
import io.coodoo.workhorse.core.entity.JobExecutionCount;
import io.coodoo.workhorse.core.entity.JobExecutionStatusSummary;
import io.coodoo.workhorse.persistence.interfaces.ExecutionPersistence;
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
public class RedisExecutionPersistence implements ExecutionPersistence {

    private static Logger log = LoggerFactory.getLogger(RedisExecutionPersistence.class);

    @Inject
    RedisController redisService;

    @Inject
    JedisExecution jedisExecution;

    @Override
    public Execution getById(Long jobId, Long executionId) {

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(executionId);
        return redisService.get(executionKey, Execution.class);
    }

    @Override
    public List<Execution> getByJobId(Long jobId, Long limit) {
        String executionByJobKey = RedisKey.LIST_OF_EXECUTION_BY_JOB.getQuery(jobId);

        List<Execution> executions = new ArrayList<>();

        List<Long> executionIds = redisService.lrange(executionByJobKey, Long.class, 0, limit);

        Map<Long, Response<String>> responseMap = getAllExecutionById(executionIds);

        for (Response<String> response : responseMap.values()) {
            Execution execution = WorkhorseUtil.jsonToParameters(response.get(), Execution.class);
            executions.add(execution);
        }

        return executions;
    }

    private Map<Long, Response<String>> getAllExecutionById(List<Long> executionIds) {

        Map<Long, Response<String>> responseMap = new HashMap<>();

        jedisExecution.execute(new JedisOperation<Long>() {

            @SuppressWarnings("unchecked")
            @Override
            public Long perform(Jedis jedis) {
                long length = 0;
                Pipeline pipeline = jedis.pipelined();

                for (Long executionId : executionIds) {
                    String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(executionId);

                    Response<String> foundExecution = pipeline.get(executionKey);
                    responseMap.put(executionId, foundExecution);
                    length++;
                }

                pipeline.sync();
                return length;
            }

        });

        return responseMap;

    }

    @Override
    public ListingResult<Execution> getExecutionListing(Long jobId, ListingParameters listingParameters) {

        String redisKey = RedisKey.LIST_OF_EXECUTION_BY_JOB.getQuery(jobId);

        List<Long> executionIds = redisService.lrange(redisKey, Long.class, 0, -1);

        Map<Long, Response<String>> responseMap = getAllExecutionById(executionIds);
        List<Execution> result = new ArrayList<>();

        for (Response<String> response : responseMap.values()) {
            Execution execution = WorkhorseUtil.jsonToParameters(response.get(), Execution.class);

            result.add(execution);

        }
        Metadata metadata = new Metadata(Long.valueOf(result.size()), listingParameters);

        return new ListingResult<Execution>(result, metadata);
    }

    @Override
    public List<Execution> pollNextExecutions(Long jobId, int limit) {

        LocalDateTime currentTimeStamp = WorkhorseUtil.timestamp();

        String listOfQueuedExecutionByJob = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(jobId, ExecutionStatus.QUEUED);

        String listOfPlannedExecutionByJob = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(jobId, ExecutionStatus.PLANNED);

        List<Execution> result = new ArrayList<>();

        // Get the executions in status Queued
        List<Long> queuedExecutionId = redisService.lrange(listOfQueuedExecutionByJob, Long.class, 0, limit - 1);

        Map<Long, Response<String>> responseMapQueued = getAllExecutionById(queuedExecutionId);

        for (Response<String> response : responseMapQueued.values()) {
            Execution execution = WorkhorseUtil.jsonToParameters(response.get(), Execution.class);
            result.add(execution);
        }

        List<Long> plannedExecutionId = redisService.lrange(listOfPlannedExecutionByJob, Long.class, 0, limit - 1);

        Map<Long, Response<String>> responseMap = getAllExecutionById(plannedExecutionId);

        for (Response<String> response : responseMap.values()) {
            Execution execution = WorkhorseUtil.jsonToParameters(response.get(), Execution.class);
            if (execution.getPlannedFor() != null && execution.getPlannedFor().isBefore(currentTimeStamp)) {
                result.add(execution);
            }
        }

        return result;
    }

    @Override
    public Execution persist(Execution execution) {
        Long executionId = redisService.incr(RedisKey.INC_EXECUTION_ID.getQuery());
        execution.setId(executionId);
        execution.setCreatedAt(WorkhorseUtil.timestamp());

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(executionId);
        redisService.set(executionKey, execution);

        // add the execution to the list of execution of the job wih the given ID
        String listOfExecutionByJobKey = RedisKey.LIST_OF_EXECUTION_BY_JOB.getQuery(execution.getJobId());
        redisService.rpush(listOfExecutionByJobKey, execution);

        // Store the parameterHash as a key with the ID of the execution as value
        Integer parameterHash = execution.getParametersHash();
        if (parameterHash != null) {
            String parameterHashKey = RedisKey.EXECUTION_BY_PARAMETER_HASH.getQuery(parameterHash);
            redisService.set(parameterHashKey, executionId);
        }

        // add the execution to the chain
        Long chainId = execution.getChainId();
        if (chainId != null && chainId > 0l) {
            String listOfExecutionOfChainId = RedisKey.LIST_OF_EXECUTION_OF_CHAIN.getQuery(chainId);
            redisService.rpush(listOfExecutionOfChainId, executionId);
        }

        // add the execution to the batch
        Long batchId = execution.getBatchId();
        if (batchId != null && batchId > 0l) {
            String listOfExecutionOfBatchId = RedisKey.LIST_OF_EXECUTION_OF_BATCH.getQuery(batchId);
            redisService.rpush(listOfExecutionOfBatchId, executionId);
        }

        // elements of an chained execution do not have to be in a queue
        if (chainId == null || chainId < 0l) {
            // add the execution to the list of the ones with the given status
            String listOfExecutionByJobOnStatus = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(execution.getJobId(), execution.getStatus());
            redisService.rpush(listOfExecutionByJobOnStatus, executionId);
        }

        return redisService.get(executionKey, Execution.class);
    }

    @Override
    public void delete(Long jobId, Long executionId) {
        // TODO Auto-generated method stub

    }

    @Override
    public Execution update(Execution execution) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Execution updateStatus(Long jobId, Long executionId, ExecutionStatus status, ExecutionFailStatus failStatus) {

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(executionId);
        Execution execution = redisService.get(executionKey, Execution.class);

        if (status.equals(execution.getStatus())) {
            return execution;
        }

        execution.setStatus(status);
        execution.setFailStatus(failStatus);

        // remove the ID of the execution in the list of her old status
        String oldListOfExecutionByJobOnStatus = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(jobId, execution.getStatus());

        redisService.lrem(oldListOfExecutionByJobOnStatus, execution.getId());

        // add the ID of the execution in the list of the new status
        String newListOfExecutionByJobOnStatus = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(jobId, status);

        redisService.rpush(newListOfExecutionByJobOnStatus, execution.getId());

        return execution;
    }

    @Override
    public int deleteOlderExecutions(Long jobId, LocalDateTime preDate) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public List<Execution> getBatch(Long jobId, Long batchId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Execution> getChain(Long jobId, Long chainId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Execution getFirstCreatedByJobIdAndParametersHash(Long jobId, Integer parameterHash) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isBatchFinished(Long jobId, Long batchId) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean abortChain(Long jobId, Long chainId) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<Execution> findTimeoutExecutions(LocalDateTime time) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<JobExecutionStatusSummary> getJobExecutionStatusSummaries(ExecutionStatus status, LocalDateTime since) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JobExecutionCount getJobExecutionCount(Long jobId, LocalDateTime from, LocalDateTime to) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ExecutionLog getLog(Long jobId, Long executionId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void log(Long jobId, Long executionId, String log) {
        // TODO Auto-generated method stub

    }

    @Override
    public void log(Long jobId, Long executionId, String error, String stacktrace) {
        // TODO Auto-generated method stub

    }

    @Override
    public void connect(Object... params) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getPersistenceName() {
        return RedisPersistenceConfig.NAME;
    }

    @Override
    public boolean isPusherAvailable() {
        return false;
    }

}
