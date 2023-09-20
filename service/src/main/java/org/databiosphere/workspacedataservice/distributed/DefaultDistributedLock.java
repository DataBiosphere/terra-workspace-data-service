package org.databiosphere.workspacedataservice.distributed;

import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.stereotype.Component;
import java.lang.InterruptedException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.TimeUnit;

@Component
public class DefaultDistributedLock implements DistributedLock {

    private final LockRegistry lockRegistry;
    private Lock lock;

    // A wrapper around a Lock for easier testing.
    public DefaultDistributedLock(LockRegistry lockRegistry) {
        this.lockRegistry = lockRegistry;
    }

    @Override
    public void obtainLock(String lockId) {
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