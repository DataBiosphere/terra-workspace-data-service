package org.databiosphere.workspacedataservice.distributed;

public interface DistributedLock {
  void obtainLock(String lockId);

  Boolean tryLock();

  void unlock();
}
