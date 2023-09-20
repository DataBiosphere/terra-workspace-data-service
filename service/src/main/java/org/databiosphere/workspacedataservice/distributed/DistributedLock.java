package org.databiosphere.workspacedataservice.distributed;

import org.springframework.integration.support.locks.LockRegistry;
import java.lang.InterruptedException;

public interface DistributedLock {
    void obtainLock(LockRegistry lockRegistry, String lockId);
    Boolean tryLock() throws InterruptedException;
    void unlock();
}