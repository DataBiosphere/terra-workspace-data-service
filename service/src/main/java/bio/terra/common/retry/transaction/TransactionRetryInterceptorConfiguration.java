package bio.terra.common.retry.transaction;

import bio.terra.common.retry.CompositeBackOffPolicy;
import java.util.LinkedHashMap;
import org.aopalliance.intercept.MethodInterceptor;
import org.databiosphere.workspacedataservice.retry.RetryLoggingListener;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.UniformRandomBackOffPolicy;
import org.springframework.retry.interceptor.RetryInterceptorBuilder;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
import org.springframework.retry.policy.CompositeRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
@EnableConfigurationProperties(TransactionRetryProperties.class)
public class TransactionRetryInterceptorConfiguration {
  /**
   * Creates an interceptor that can be used in {@link
   * org.springframework.retry.annotation.Retryable}: <code>
   * @Retryable(interceptor = "transactionRetryInterceptor")</code>. Be sure to use {@link
   * org.springframework.retry.annotation.EnableRetry}.
   */
  @Bean("transactionRetryInterceptor")
  public MethodInterceptor getTransactionRetryInterceptor(TransactionRetryProperties config) {
    RetryPolicy retryPolicy = createTransactionRetryPolicy(config);
    BackOffPolicy backOffPolicy = createTransactionBackOffPolicy(config);

    RetryTemplate retryTemplate =
        RetryTemplate.builder()
            .customBackoff(backOffPolicy)
            .customPolicy(retryPolicy)
            .withListener(new RetryLoggingListener())
            .build();

    RetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless().build();
    interceptor.setRetryOperations(retryTemplate);
    return interceptor;
  }

  /**
   * Fast retries with random delay between config.getFastRetryMinBackOffPeriod and
   * config.getFastRetryMaxBackOffPeriod. Slow retries with exponential back off with initial
   * config.getSlowRetryInitialInterval delay, multiplied by config.getSlowRetryMultiplier each
   * attempt.
   */
  private BackOffPolicy createTransactionBackOffPolicy(TransactionRetryProperties config) {
    UniformRandomBackOffPolicy fastBackOffPolicy = new UniformRandomBackOffPolicy();
    fastBackOffPolicy.setMaxBackOffPeriod(config.getFastRetryMaxBackOffPeriod().toMillis());
    fastBackOffPolicy.setMinBackOffPeriod(config.getFastRetryMinBackOffPeriod().toMillis());

    ExponentialBackOffPolicy slowBackOffPolicy = new ExponentialBackOffPolicy();
    slowBackOffPolicy.setInitialInterval(config.getSlowRetryInitialInterval().toMillis());
    slowBackOffPolicy.setMultiplier(config.getSlowRetryMultiplier());

    LinkedHashMap<BinaryExceptionClassifier, BackOffPolicy> backOffPolicies = new LinkedHashMap<>();

    backOffPolicies.put(config.getFastRetryExceptionClassifier(), fastBackOffPolicy);
    backOffPolicies.put(config.getSlowRetryExceptionClassifier(), slowBackOffPolicy);

    return new CompositeBackOffPolicy(backOffPolicies);
  }

  /** Policy dictating number of attempts for fast and slow retries. */
  private RetryPolicy createTransactionRetryPolicy(TransactionRetryProperties config) {
    CompositeRetryPolicy retryPolicy = new CompositeRetryPolicy();
    retryPolicy.setOptimistic(true); // retry when any nested policy says to retry
    retryPolicy.setPolicies(
        new RetryPolicy[] {
          new SimpleRetryPolicy(
              config.getFastRetryMaxAttempts(), config.getFastRetryExceptionClassifier()),
          new SimpleRetryPolicy(
              config.getSlowRetryMaxAttempts(), config.getSlowRetryExceptionClassifier())
        });
    return retryPolicy;
  }
}
