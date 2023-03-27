package org.databiosphere.workspacedataservice.retry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.transaction.CannotCreateTransactionException;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
class TransactionRetryTest {

    @Autowired TransactionRetryTestBean transactionRetryTestBean;

    @BeforeEach
    public void beforeEach() {
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
        Exception ex = assertThrows(clazz,
                () -> transactionRetryTestBean.failingWriteTransaction(toThrow));
        assertInstanceOf(clazz, ex);
        // with current settings, will retry 4 times. Any retry means we'll have more than
        // one invocation.
        assertTrue(transactionRetryTestBean.getCount() > 1,
                "transaction should have been attempted multiple times");
    }

    @ParameterizedTest(name = "@ReadTransaction should retry on {0}")
    @MethodSource("provideExceptionClasses")
    void doReadTransactionsRetry(Class<? extends Exception> clazz) throws Exception {
        Exception toThrow = clazz.getDeclaredConstructor(String.class).newInstance("unit test");

        assertEquals(0, transactionRetryTestBean.getCount());
        Exception ex = assertThrows(clazz,
                () -> transactionRetryTestBean.failingReadTransaction(toThrow));
        assertInstanceOf(clazz, ex);
        // with current settings, will retry 4 times. Any retry means we'll have more than
        // one invocation.
        assertTrue(transactionRetryTestBean.getCount() > 1,
                "transaction should have been attempted multiple times");

    }


}
