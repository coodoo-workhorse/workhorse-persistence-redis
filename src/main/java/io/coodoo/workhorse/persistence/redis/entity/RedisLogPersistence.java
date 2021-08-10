package io.coodoo.workhorse.persistence.redis.entity;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import io.coodoo.workhorse.core.entity.WorkhorseLog;
import io.coodoo.workhorse.persistence.interfaces.LogPersistence;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingParameters;
import io.coodoo.workhorse.persistence.interfaces.listing.ListingResult;

@ApplicationScoped
public class RedisLogPersistence implements LogPersistence {

    @Override
    public WorkhorseLog get(Long logId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WorkhorseLog update(Long logId, WorkhorseLog workhorseLog) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WorkhorseLog delete(Long logId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WorkhorseLog persist(WorkhorseLog workhorseLog) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<WorkhorseLog> getAll(int limit) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ListingResult<WorkhorseLog> getWorkhorseLogListing(ListingParameters listingParameters) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int deleteByJobId(Long jobId) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getPersistenceName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void connect(Object... params) {
        // TODO Auto-generated method stub

    }

}
