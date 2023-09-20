package org.databiosphere.workspacedataservice.distributed;

import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.stereotype.Component;
import java.lang.InterruptedException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.TimeUnit;

@Component
public class DefaultDistributedLock implements DistributedLock {
    
    private Lock lock;

    // A wrapper around a Lock for easier testing.
    public DefaultDistributedLock() {}

    @Override
    public void obtainLock(LockRegistry lockRegistry, String lockId) {
        lock = lockRegistry.obtain(lockId);
    }

    @Override
    public Boolean tryLock() throws InterruptedException {
        if(lock == null) {
            return false;
        }
        try {
            return lock.tryLock(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw e;
        }
    }

    @Override
    public void unlock() {
        if(lock == null) {
            return;
        }
        lock.unlock();
    }
}