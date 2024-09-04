package org.databiosphere.workspacedataservice.retry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.CannotCreateTransactionException;

@DirtiesContext
// aggressive retry settings to make this test fast; these are
// inappropriate for runtime behavior.
@SpringBootTest(
    properties = {
      "terra.common.retry.transaction.slowRetryMaxAttempts=2",
      "terra.common.retry.transaction.slowRetryInitialInterval=250ms",
      "terra.common.retry.transaction.slowRetryMultiplier=1.1",
      "terra.common.retry.transaction.fastRetryMaxAttempts=2"
    })
class TransactionRetryTest extends DataPlaneTestBase {

  @Autowired TransactionRetryTestBean transactionRetryTestBean;

  @BeforeEach
  public void setUp() {
    transactionRetryTestBean.resetCount();
  }

  private static Stream<Arguments> provideExceptionClasses() {
    return Stream.of(
        Arguments.of(RecoverableDataAccessException.class),
        Arguments.of(CannotCreateTransactionException.class),
        Arguments.of(ConcurrencyFailureException.class), // extends TransientDataAccessException
        Arguments.of(QueryTimeoutException.class) // extends TransientDataAccessException
        );
  }

  @ParameterizedTest(name = "@WriteTransaction should retry on {0}")
  @MethodSource("provideExceptionClasses")
  void doWriteTransactionsRetry(Class<? extends Exception> clazz) throws Exception {
    Exception toThrow = clazz.getDeclaredConstructor(String.class).newInstance("unit test");

    assertEquals(0, transactionRetryTestBean.getCount());
    Exception ex =
        assertThrows(clazz, () -> transactionRetryTestBean.failingWriteTransaction(toThrow));
    assertInstanceOf(clazz, ex);
    // with current settings, will retry 4 times. Any retry means we'll have more than
    // one invocation.
    assertTrue(
        transactionRetryTestBean.getCount() > 1,
        "transaction should have been attempted multiple times");
  }

  @ParameterizedTest(name = "@ReadTransaction should retry on {0}")
  @MethodSource("provideExceptionClasses")
  void doReadTransactionsRetry(Class<? extends Exception> clazz) throws Exception {
    Exception toThrow = clazz.getDeclaredConstructor(String.class).newInstance("unit test");

    assertEquals(0, transactionRetryTestBean.getCount());
    Exception ex =
        assertThrows(clazz, () -> transactionRetryTestBean.failingReadTransaction(toThrow));
    assertInstanceOf(clazz, ex);
    // with current settings, will retry 4 times. Any retry means we'll have more than
    // one invocation.
    assertTrue(
        transactionRetryTestBean.getCount() > 1,
        "transaction should have been attempted multiple times");
  }
}
