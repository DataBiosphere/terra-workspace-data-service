package org.databiosphere.workspacedataservice.distributed;

public class MockFailedDistributedLock implements DistributedLock {

  // This class is a standin for a Lock that fails to retrieve.
  public MockFailedDistributedLock() {}

  @Override
  public Lock obtainLock(String lockId) {}

  @Override
  public Boolean tryLock(Lock lock) {
    return false;
  }

  @Override
  public void unlock(Lock lock) {}
}
