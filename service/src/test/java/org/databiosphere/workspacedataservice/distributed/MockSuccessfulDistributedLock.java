package org.databiosphere.workspacedataservice.distributed;

import org.springframework.integration.support.locks.LockRegistry;

public class MockSuccessfulDistributedLock implements DistributedLock {

    // This class is a standin for a Lock that successfully retrieves.
    public MockSuccessfulDistributedLock() {}

    @Override
    public void obtainLock(LockRegistry lockRegistry, String lockId) {}

    @Override
    public Boolean tryLock() {
        return true;
    }

    @Override
    public void unlock() {}
}