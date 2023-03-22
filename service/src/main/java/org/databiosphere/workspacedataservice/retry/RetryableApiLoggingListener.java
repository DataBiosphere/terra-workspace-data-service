package org.databiosphere.workspacedataservice.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.stereotype.Component;

@Component("retryableApiLoggingListener")
public class RetryableApiLoggingListener extends RetryListenerSupport {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {

        logger.warn("Retryable method {} threw {}th exception {}",
                context.getAttribute("context.name"), context.getRetryCount(), throwable.toString());
    }
}
