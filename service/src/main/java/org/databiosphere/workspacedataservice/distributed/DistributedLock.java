package org.databiosphere.workspacedataservice.distributed;

import java.lang.InterruptedException;

public interface DistributedLock {
    void obtainLock(String lockId);
    Boolean tryLock() throws InterruptedException;
    void unlock();
}