package org.databiosphere.workspacedataservice.distributed;

public class MockSuccessfulDistributedLock implements DistributedLock {

  // This class is a standin for a Lock that successfully retrieves.
  public MockSuccessfulDistributedLock() {}

  @Override
  public void obtainLock(String lockId) {}

  @Override
  public Boolean tryLock() {
    return true;
  }

  @Override
  public void unlock() {}
}
