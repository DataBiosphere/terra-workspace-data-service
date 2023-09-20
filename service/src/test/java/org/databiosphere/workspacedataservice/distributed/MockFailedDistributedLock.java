package org.databiosphere.workspacedataservice.distributed;

import org.springframework.integration.support.locks.LockRegistry;

public class MockFailedDistributedLock implements DistributedLock {

    // This class is a standin for a Lock that fails to retrieve.
    public MockFailedDistributedLock() {}

    @Override
    public void obtainLock(LockRegistry lockRegistry, String lockId) {}

    @Override
    public Boolean tryLock() {
        return false;
    }

    @Override
    public void unlock() {}
}