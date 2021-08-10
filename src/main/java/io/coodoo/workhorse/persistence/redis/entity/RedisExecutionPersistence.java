package io.coodoo.workhorse.persistence.redis.entity;

import java.time.LocalDateTime;
import java.util.List;

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
import io.coodoo.workhorse.persistence.redis.control.RedisController;

@ApplicationScoped
public class RedisExecutionPersistence implements ExecutionPersistence {

    private static Logger log = LoggerFactory.getLogger(RedisExecutionPersistence.class);

    @Inject
    RedisController redisService;

    @Override
    public Execution getById(Long jobId, Long executionId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Execution> getByJobId(Long jobId, Long limit) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListingResult<Execution> getExecutionListing(Long jobId, ListingParameters listingParameters) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Execution> pollNextExecutions(Long jobId, int limit) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Execution persist(Execution execution) {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isPusherAvailable() {
        // TODO Auto-generated method stub
        return false;
    }

}
