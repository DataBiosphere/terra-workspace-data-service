package org.databiosphere.workspacedataservice.distributed;

import java.util.concurrent.locks.Lock;
import org.mockito.Mockito;

public class MockFailedDistributedLock implements DistributedLock {

  // This class is a standin for a Lock that fails to retrieve.
  public MockFailedDistributedLock() {}

  @Override
  public Lock obtainLock(String lockId) {
    return Mockito.mock(Lock.class);
  }

  @Override
  public Boolean tryLock(Lock lock) {
    return false;
  }

  @Override
  public void unlock(Lock lock) {}
}
