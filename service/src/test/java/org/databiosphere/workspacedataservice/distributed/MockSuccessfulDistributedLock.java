package org.databiosphere.workspacedataservice.distributed;

public class MockSuccessfulDistributedLock implements DistributedLock {

  // This class is a standin for a Lock that successfully retrieves.
  public MockSuccessfulDistributedLock() {}

  @Override
  public Lock obtainLock(String lockId) {}

  @Override
  public Boolean tryLock(Lock lock) {
    return true;
  }

  @Override
  public void unlock(Lock lock) {}
}
