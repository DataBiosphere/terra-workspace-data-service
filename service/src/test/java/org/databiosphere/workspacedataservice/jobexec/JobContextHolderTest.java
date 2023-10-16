package org.databiosphere.workspacedataservice.jobexec;

import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

class JobContextHolderTest {

  @Test
  void threadIsolation() throws InterruptedException {
    // how many threads should we run in this test
    var numThreads = 5;
    // var to hold the actual outputs from each thread
    Set<String> actualValues = ConcurrentHashMap.newKeySet();
    // semaphore to track when all threads are done
    CountDownLatch latch = new CountDownLatch(numThreads);

    // generate {numThreads} random String values; these are the values we'll eventually assert on
    List<String> randomStrings =
        IntStream.rangeClosed(1, numThreads)
            .mapToObj(i -> RandomStringUtils.randomAlphabetic(8))
            .toList();

    /* ----- inner class: the thread to execute during the test ----- */
    class UnitTestThread implements Runnable {
      private final CountDownLatch latch;
      private final String randValue;

      public UnitTestThread(CountDownLatch latch, String randValue) {
        this.latch = latch;
        this.randValue = randValue;
      }

      // represents what a QuartzJob would do: set a JobContextHolder value; later get that value.
      @Override
      public void run() {
        try {
          // init the JobContextHolder and set a randValue into it
          JobContextHolder.init();
          JobContextHolder.setAttribute("unitTestKey", randValue);
          // sleep briefly - this ensures all threads in this test have had a chance
          // to set their values before we start retrieving values
          Thread.sleep(500);
          // get the value that we just set. Since this is local to the thread, this
          // value should be exactly what we set it to above; any other threads that also
          // called JobContextHolder.setAttribute shouldn't affect our value.
          var actualValue = JobContextHolder.getAttribute("unitTestKey");
          // validate the actualValue; if valid, add it to the actualValues set
          if (actualValue instanceof String strVal) {
            actualValues.add(strVal);
          } else {
            fail("Failure in test thread; actualValue was [{}]", actualValue);
          }
          // clean up; don't leave values on the thread (memory leaks, interference with other
          // tests)
          JobContextHolder.destroy();
          // finally, tell the latch that this thread is done
          latch.countDown();
        } catch (InterruptedException e) {
          fail("Failure in test thread: " + e.getMessage());
        }
      }
    }
    /* ----- end inner class ----- */

    // create {numThreads} threads, each thread is a UnitTestThread
    List<Thread> testThreads =
        randomStrings.stream()
            .map(randValue -> new Thread(new UnitTestThread(latch, randValue)))
            .toList();

    // start all the threads
    testThreads.forEach(Thread::start);

    // wait for all threads to finish
    boolean finished = latch.await(60, TimeUnit.SECONDS);
    if (!finished) {
      fail("Threads did not complete!");
    }

    // finally, assert that the values found by the threads were the same as the inputs
    assertEquals(new HashSet<>(randomStrings), actualValues);
  }
}
