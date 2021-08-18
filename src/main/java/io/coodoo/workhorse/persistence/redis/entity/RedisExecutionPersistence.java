package io.coodoo.workhorse.persistence.redis.entity;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.coodoo.workhorse.core.control.event.NewExecutionEvent;
import io.coodoo.workhorse.core.entity.Execution;
import io.coodoo.workhorse.core.entity.ExecutionFailStatus;
import io.coodoo.workhorse.core.entity.ExecutionLog;
import io.coodoo.workhorse.core.entity.ExecutionStatus;
import io.coodoo.workhorse.core.entity.Job;
import io.coodoo.workhorse.core.entity.JobExecutionCount;
import io.coodoo.workhorse.core.entity.JobExecutionStatusSummary;
import io.coodoo.workhorse.persistence.interfaces.ExecutionPersistence;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingParameters;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingResult;
import io.coodoo.workhorse.persistence.interfaces.listing.Metadata;
import io.coodoo.workhorse.persistence.redis.boundary.RedisPersistenceConfig;
import io.coodoo.workhorse.persistence.redis.boundary.StaticRedisConfig;
import io.coodoo.workhorse.persistence.redis.control.JedisExecution;
import io.coodoo.workhorse.persistence.redis.control.JedisOperation;
import io.coodoo.workhorse.persistence.redis.control.RedisKey;
import io.coodoo.workhorse.persistence.redis.control.RedisService;
import io.coodoo.workhorse.util.WorkhorseUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 * @author coodoo GmbH (coodoo.io)
 */
@ApplicationScoped
public class RedisExecutionPersistence implements ExecutionPersistence {

    private static Logger log = LoggerFactory.getLogger(RedisExecutionPersistence.class);

    @Inject
    RedisService redisService;

    @Inject
    JedisExecution jedisExecution;

    @Inject
    Event<NewExecutionEvent> newExecutionEventEvent;

    @Override
    public Execution getById(Long jobId, Long executionId) {

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(executionId);
        return redisService.get(executionKey, Execution.class);
    }

    @Override
    public List<Execution> getByJobId(Long jobId, Long limit) {
        String executionByJobKey = RedisKey.LIST_OF_EXECUTION_BY_JOB.getQuery(jobId);

        List<Long> executionIds = redisService.lrange(executionByJobKey, Long.class, 0, limit);

        List<String> executionIdKeys = new ArrayList<>();

        for (Long executionId : executionIds) {

            executionIdKeys.add(RedisKey.EXECUTION_BY_ID.getQuery(executionId));
        }

        return redisService.get(executionIdKeys, Execution.class);
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

        long start = listingParameters.getIndex();
        long end = listingParameters.getIndex() + listingParameters.getLimit() - 1;
        List<Long> executionIds = redisService.lrange(redisKey, Long.class, start, end);

        List<String> executionIdKeys = new ArrayList<>();
        List<Execution> result = new ArrayList<>();

        for (Long executionId : executionIds) {

            executionIdKeys.add(RedisKey.EXECUTION_BY_ID.getQuery(executionId));
        }

        result = redisService.get(executionIdKeys, Execution.class);

        long size = redisService.llen(redisKey);
        Metadata metadata = new Metadata(size, listingParameters);

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

        // add the execution to the list of execution of the job with the given ID
        String listOfExecutionByJobKey = RedisKey.LIST_OF_EXECUTION_BY_JOB.getQuery(execution.getJobId());
        redisService.lpush(listOfExecutionByJobKey, executionId);

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

        newExecutionEventEvent.fireAsync(new NewExecutionEvent(execution.getJobId(), execution.getId()));
        return redisService.get(executionKey, Execution.class);
    }

    @Override
    public void delete(Long jobId, Long executionId) {

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(executionId);
        Execution execution = redisService.get(executionKey, Execution.class);

        // remove the parameterHash as a key with the ID of the execution as value
        Integer parameterHash = execution.getParametersHash();
        if (parameterHash != null) {
            String parameterHashKey = RedisKey.EXECUTION_BY_PARAMETER_HASH.getQuery(parameterHash);
            redisService.del(parameterHashKey);
        }

        // remove the execution in the chain
        Long chainId = execution.getChainId();
        if (chainId != null && chainId > 0l) {
            String listOfExecutionOfChainId = RedisKey.LIST_OF_EXECUTION_OF_CHAIN.getQuery(chainId);
            redisService.lrem(listOfExecutionOfChainId, executionId);
        }

        // add the execution to the batch
        Long batchId = execution.getBatchId();
        if (batchId != null && batchId > 0l) {
            String listOfExecutionOfBatchId = RedisKey.LIST_OF_EXECUTION_OF_BATCH.getQuery(batchId);
            redisService.lrem(listOfExecutionOfBatchId, executionId);
        }

        // remove the execution ID in the list of executions in the current status
        String executionStatusKey = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(execution.getStatus());
        redisService.lrem(executionStatusKey, executionId);

        // remove the execution to the list of execution of the job with the given ID
        String listOfExecutionByJobKey = RedisKey.LIST_OF_EXECUTION_BY_JOB.getQuery(jobId);
        redisService.lrem(listOfExecutionByJobKey, executionId);

        // remove the execution
        redisService.del(executionKey);
    }

    @Override
    public Execution update(Execution execution) {

        Long executionId = execution.getId();

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(executionId);

        Execution oldExecution = redisService.get(executionKey, Execution.class);

        Long chainId = execution.getChainId();
        if (chainId != null && chainId > 0l) {
            String listOfExecutionOfChainId = RedisKey.LIST_OF_EXECUTION_OF_CHAIN.getQuery(chainId);
            redisService.rpush(listOfExecutionOfChainId, executionId);
        }

        Long batchId = execution.getBatchId();
        if (batchId != null && batchId > 0l) {
            String listOfExecutionOfBatchId = RedisKey.LIST_OF_EXECUTION_OF_BATCH.getQuery(batchId);
            redisService.rpush(listOfExecutionOfBatchId, executionId);
        }

        if (!Objects.equals(oldExecution.getStatus(), execution.getStatus())) {

            Long jobId = execution.getJobId();
            // remove the ID of the execution in the list of her old status
            String oldListOfExecutionByJobOnStatus = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(jobId, oldExecution.getStatus());
            // add the ID of the execution in the list of the new status
            String newListOfExecutionByJobOnStatus = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(jobId, execution.getStatus());

            redisService.lmove(oldListOfExecutionByJobOnStatus, newListOfExecutionByJobOnStatus, executionId);

        }

        execution.setUpdatedAt(WorkhorseUtil.timestamp());
        redisService.set(executionKey, execution);

        return execution;
    }

    @Override
    public Execution updateStatus(Long jobId, Long executionId, ExecutionStatus status, ExecutionFailStatus failStatus) {

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(executionId);
        Execution execution = redisService.get(executionKey, Execution.class);

        if (status.equals(execution.getStatus())) {
            return execution;
        }

        execution.setStatus(status);

        if (failStatus == null) {
            failStatus = ExecutionFailStatus.NONE;
        }
        execution.setFailStatus(failStatus);

        return update(execution);

    }

    @Override
    public int deleteOlderExecutions(Long jobId, LocalDateTime preDate) {

        int count = 0;
        // get all executions
        List<Execution> executions = getByJobId(jobId, -1l);

        for (Execution execution : executions) {

            if (execution.getCreatedAt().isBefore(preDate)) {
                delete(jobId, execution.getId());
                count++;
            }

        }
        return count;
    }

    @Override
    public List<Execution> getBatch(Long jobId, Long batchId) {
        String listOfExecutionOfBatchId = RedisKey.LIST_OF_EXECUTION_OF_BATCH.getQuery(batchId);

        List<Execution> result = new ArrayList<>();

        // Get the executions in the batch
        List<Long> batchExecutionIds = redisService.lrange(listOfExecutionOfBatchId, Long.class, 0, -1);

        Map<Long, Response<String>> responseMapQueued = getAllExecutionById(batchExecutionIds);

        for (Response<String> response : responseMapQueued.values()) {
            Execution execution = WorkhorseUtil.jsonToParameters(response.get(), Execution.class);
            result.add(execution);
        }

        return result;
    }

    @Override
    public List<Execution> getChain(Long jobId, Long chainId) {

        String listOfExecutionOfChainId = RedisKey.LIST_OF_EXECUTION_OF_CHAIN.getQuery(chainId);

        List<Execution> result = new ArrayList<>();

        // get the executions in the batch
        List<Long> chainExecutionIds = redisService.lrange(listOfExecutionOfChainId, Long.class, 0, -1);

        Map<Long, Response<String>> responseMapQueued = getAllExecutionById(chainExecutionIds);

        for (Response<String> response : responseMapQueued.values()) {
            Execution execution = WorkhorseUtil.jsonToParameters(response.get(), Execution.class);
            result.add(execution);
        }

        return result;
    }

    @Override
    public Execution getFirstCreatedByJobIdAndParametersHash(Long jobId, Integer parameterHash) {
        String parameterHashKey = RedisKey.EXECUTION_BY_PARAMETER_HASH.getQuery(parameterHash);

        Long executionId = redisService.get(parameterHashKey, Long.class);

        if (executionId == null) {
            return null;
        }

        String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(executionId);
        Execution execution = redisService.get(executionKey, Execution.class);

        if (ExecutionStatus.QUEUED.equals(execution.getStatus())) {
            return execution;
        }

        return null;
    }

    @Override
    public boolean isBatchFinished(Long jobId, Long batchId) {

        // get the IDs of all executions of the given batch
        String listOfBatchKey = RedisKey.LIST_OF_EXECUTION_OF_BATCH.getQuery(batchId);
        List<Long> batchExecutionIds = redisService.lrange(listOfBatchKey, Long.class, 0, -1);

        // build the rediskey used to get execution by ID
        List<String> executionKeys = new ArrayList<>();
        for (Long executionId : batchExecutionIds) {
            executionKeys.add(RedisKey.EXECUTION_BY_ID.getQuery(executionId));
        }

        // get all executions of this batch
        List<Execution> executions = redisService.get(executionKeys, Execution.class);

        // if at least one execution is queued, the batch is not finished
        for (Execution execution : executions) {
            if (ExecutionStatus.QUEUED.equals(execution.getStatus())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean abortChain(Long jobId, Long chainId) {

        // get the IDs of all executions of the given chain
        String listOfExecutionOfChainId = RedisKey.LIST_OF_EXECUTION_OF_CHAIN.getQuery(chainId);
        List<Long> chainExecutionIds = redisService.lrange(listOfExecutionOfChainId, Long.class, 0, -1);

        // set the queued executions of the chain to status ABORTED
        for (Long executionId : chainExecutionIds) {
            String executionKey = RedisKey.EXECUTION_BY_ID.getQuery(executionId);
            Execution execution = redisService.get(executionKey, Execution.class);
            if (ExecutionStatus.QUEUED.equals(execution.getStatus())) {
                updateStatus(jobId, executionId, ExecutionStatus.ABORTED, ExecutionFailStatus.NONE);
            }
        }

        return true;
    }

    @Override
    public List<Execution> findTimeoutExecutions(LocalDateTime time) {
        String redisKey = RedisKey.LIST_OF_JOB.getQuery();

        List<Execution> result = new ArrayList<>();
        List<Long> jobIds = redisService.lrange(redisKey, Long.class, 0, -1);

        for (Long jobId : jobIds) {

            List<Execution> executions = getByJobId(jobId, -1l);

            for (Execution execution : executions) {
                if (ExecutionStatus.RUNNING.equals(execution.getStatus()) && execution.getUpdatedAt().isBefore(time)) {
                    result.add(execution);
                }
            }

        }

        return result;
    }

    @Override
    public List<JobExecutionStatusSummary> getJobExecutionStatusSummaries(ExecutionStatus status, LocalDateTime since) {

        // The parameter -since- is not considered due to performance purpose

        List<JobExecutionStatusSummary> result = new ArrayList<>();

        String listOfJobsKey = RedisKey.LIST_OF_JOB.getQuery();

        List<Long> jobIds = redisService.lrange(listOfJobsKey, Long.class, 0, -1);

        for (Long jobId : jobIds) {
            String listOfExecutionIdInStatus = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(jobId, status);

            List<Long> executionIds = redisService.lrange(listOfExecutionIdInStatus, Long.class, 0, -1);

            if (executionIds != null && executionIds.size() > 0) {
                String jobKey = RedisKey.JOB_BY_ID.getQuery(jobId);
                Job job = redisService.get(jobKey, Job.class);
                result.add(new JobExecutionStatusSummary(status, Long.valueOf(executionIds.size()), job));
            }
        }

        return result;
    }

    @Override
    public JobExecutionCount getJobExecutionCount(Long jobId, LocalDateTime from, LocalDateTime to) {

        List<Long> jobIds = new ArrayList<>();

        if (jobId == null) {
            String listOfJobsKey = RedisKey.LIST_OF_JOB.getQuery();

            List<Long> redisJobIds = redisService.lrange(listOfJobsKey, Long.class, 0, -1);
            jobIds.addAll(redisJobIds);
        } else {
            jobIds.add(jobId);
        }

        long countPlanned = 0L;
        long countRunning = 0L;
        long countFinished = 0L;
        long countFailed = 0L;
        long countAbort = 0L;
        long countQueued = 0L;

        for (Long id : jobIds) {

            String listOfExecutionsPlanned = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(id, ExecutionStatus.PLANNED);
            countPlanned = countPlanned + redisService.llen(listOfExecutionsPlanned);

            String listOfExecutionsQueued = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(id, ExecutionStatus.QUEUED);
            countQueued = countQueued + redisService.llen(listOfExecutionsQueued);

            String listOfExecutionsRunning = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(id, ExecutionStatus.RUNNING);
            countRunning = countRunning + redisService.llen(listOfExecutionsRunning);

            String listOfExecutionsFinished = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(id, ExecutionStatus.FINISHED);
            countFinished = countFinished + redisService.llen(listOfExecutionsFinished);

            String listOfExecutionsFailed = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(id, ExecutionStatus.FAILED);
            countFailed = countFailed + redisService.llen(listOfExecutionsFailed);

            String listOfExecutionsAborted = RedisKey.LIST_OF_EXECUTION_OF_JOB_BY_STATUS.getQuery(id, ExecutionStatus.ABORTED);
            countAbort = countAbort + redisService.llen(listOfExecutionsAborted);
        }

        return new JobExecutionCount(jobId, from, to, countPlanned, countQueued, countRunning, countFinished, countFailed, countAbort);
    }

    @Override
    public ExecutionLog getLog(Long jobId, Long executionId) {

        String executionErrorKey = RedisKey.EXECUTION_ERROR_AND_STACKTRACE_BY_ID.getQuery(executionId);

        String executionLogKey = RedisKey.LIST_OF_EXECUTION_LOG_BY_ID.getQuery(executionId);

        ExecutionLog executionLog = new ExecutionLog();
        executionLog.setId(executionId);
        executionLog.setExecutionId(executionId);

        ExecutionLog executionError = redisService.get(executionErrorKey, ExecutionLog.class);

        if (executionError != null) {

            executionLog.setError(executionError.getError());
            executionLog.setStacktrace(executionError.getStacktrace());
        }

        // get all logs of the list
        List<String> executionLogs = redisService.lrange(executionLogKey, String.class, 0, -1);
        StringBuilder bufferlog = new StringBuilder();

        // append the logs of the list to build one string
        for (String log : executionLogs) {
            bufferlog.append(log);
            bufferlog.append(System.lineSeparator());
        }

        // set the builded string to ExecutionLog.log
        executionLog.setLog(bufferlog.toString());

        return executionLog;

    }

    @Override
    public void log(Long jobId, Long executionId, String log) {

        String executionLogKey = RedisKey.LIST_OF_EXECUTION_LOG_BY_ID.getQuery(executionId);

        redisService.lpush(executionLogKey, log);

    }

    @Override
    public void log(Long jobId, Long executionId, String error, String stacktrace) {

        String executionLogKey = RedisKey.EXECUTION_ERROR_AND_STACKTRACE_BY_ID.getQuery(executionId);

        ExecutionLog executionLog = redisService.get(executionLogKey, ExecutionLog.class);

        if (executionLog == null) {

            executionLog = new ExecutionLog();
            executionLog.setId(executionId);
            executionLog.setExecutionId(executionId);
            executionLog.setCreatedAt(WorkhorseUtil.timestamp());
        } else {
            executionLog.setUpdatedAt(WorkhorseUtil.timestamp());
        }

        executionLog.setError(error);
        executionLog.setStacktrace(stacktrace);

        redisService.set(executionLogKey, executionLog);
    }

    @Override
    public void connect(Object... params) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getPersistenceName() {
        return StaticRedisConfig.NAME;
    }

    @Override
    public boolean isPusherAvailable() {
        return true;
    }

}
