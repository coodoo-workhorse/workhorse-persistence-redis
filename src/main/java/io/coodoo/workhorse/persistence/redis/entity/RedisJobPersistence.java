package io.coodoo.workhorse.persistence.redis.entity;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import io.coodoo.workhorse.core.entity.Job;
import io.coodoo.workhorse.core.entity.JobStatus;
import io.coodoo.workhorse.core.entity.JobStatusCount;
import io.coodoo.workhorse.persistence.interfaces.JobPersistence;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingParameters;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingResult;

@ApplicationScoped
public class RedisJobPersistence implements JobPersistence {

    @Override
    public Job get(Long jobId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Job getByName(String jobName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Job getByWorkerClassName(String jobClassName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Job> getAll() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListingResult<Job> getJobListing(ListingParameters listingParameters) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Job> getAllByStatus(JobStatus jobStatus) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Job> getAllScheduled() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JobStatusCount getJobStatusCount() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long count() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long countByStatus(JobStatus jobStatus) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Job persist(Job job) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Job update(Job job) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void deleteJob(Long jobId) {
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

}
