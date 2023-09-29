package org.databiosphere.workspacedataservice.distributed;

import java.util.concurrent.locks.Lock;
import org.mockito.Mockito;

public class MockSuccessfulDistributedLock implements DistributedLock {

  // This class is a standin for a Lock that successfully retrieves.
  public MockSuccessfulDistributedLock() {}

  @Override
  public Lock obtainLock(String lockId) {
    return Mockito.mock(Lock.class);
  }

  @Override
  public Boolean tryLock(Lock lock) {
    return true;
  }

  @Override
  public void unlock(Lock lock) {}
}
