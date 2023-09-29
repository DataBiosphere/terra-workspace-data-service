package org.databiosphere.workspacedataservice.distributed;

import java.util.concurrent.locks.Lock;

public interface DistributedLock {
  Lock obtainLock(String lockId);

  Boolean tryLock(Lock lock);

  void unlock(Lock lock);
}
