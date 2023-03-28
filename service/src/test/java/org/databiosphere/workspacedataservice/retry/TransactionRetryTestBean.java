package org.databiosphere.workspacedataservice.retry;

import bio.terra.common.db.ReadTransaction;
import bio.terra.common.db.WriteTransaction;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Support for TransactionRetryTest; needs to be a separate class from TransactionRetryTest
 * to allow Spring to create proxies and implement the transactions + retries
 */
@Component("transactionRetryTestBean")
public class TransactionRetryTestBean {

    private final AtomicInteger count = new AtomicInteger(0);

    public int resetCount() {
        count.set(0);
        return count.get();
    }

    public int getCount() {
        return count.get();
    }

    @WriteTransaction
    public int failingWriteTransaction(Exception e) throws Exception {
        count.incrementAndGet();
        throw e;
    }

    @ReadTransaction
    public int failingReadTransaction(Exception e) throws Exception {
        count.incrementAndGet();
        throw e;
    }


}
