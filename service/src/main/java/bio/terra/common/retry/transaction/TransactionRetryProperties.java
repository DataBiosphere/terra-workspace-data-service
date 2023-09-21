package bio.terra.common.retry.transaction;

import java.time.Duration;
import java.util.List;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.CannotCreateTransactionException;

/**
 * Configuration settings for how database transactions are retried. There are 2 classes of retries,
 * fast and slow. Fast retries usually measure in milliseconds with a constant retry period (with
 * some random jitter) and many attempts. Slow retries usually measure in seconds with an
 * exponential back off and few attempts.
 */
@ConfigurationProperties(prefix = "terra.common.retry.transaction")
public class TransactionRetryProperties implements InitializingBean {
  private List<Class<? extends Throwable>> fastRetryExceptions =
      List.of(TransientDataAccessException.class);
  private Integer fastRetryMaxAttempts = 100;
  private Duration fastRetryMinBackOffPeriod = Duration.ofMillis(10);
  private Duration fastRetryMaxBackOffPeriod = Duration.ofMillis(20);

  private List<Class<? extends Throwable>> slowRetryExceptions =
      List.of(RecoverableDataAccessException.class, CannotCreateTransactionException.class);
  private Integer slowRetryMaxAttempts = 4;
  private Duration slowRetryInitialInterval = Duration.ofSeconds(1);
  private Double slowRetryMultiplier = 2.0;

  private BinaryExceptionClassifier slowRetryExceptionClassifier;
  private BinaryExceptionClassifier fastRetryExceptionClassifier;

  /** Exceptions to retry FAST */
  public List<Class<? extends Throwable>> getFastRetryExceptions() {
    return fastRetryExceptions;
  }

  public void setFastRetryExceptions(List<Class<? extends Throwable>> fastRetryExceptions) {
    this.fastRetryExceptions = fastRetryExceptions;
  }

  /** Max attempts for FAST retries (including initial attempt) */
  public Integer getFastRetryMaxAttempts() {
    return fastRetryMaxAttempts;
  }

  public void setFastRetryMaxAttempts(Integer fastRetryMaxAttempts) {
    this.fastRetryMaxAttempts = fastRetryMaxAttempts;
  }

  /** Minimum time to wait before the next FAST retry */
  public Duration getFastRetryMinBackOffPeriod() {
    return fastRetryMinBackOffPeriod;
  }

  public void setFastRetryMinBackOffPeriod(Duration fastRetryMinBackOffPeriod) {
    this.fastRetryMinBackOffPeriod = fastRetryMinBackOffPeriod;
  }

  /** Maximum time to wait before the next FAST retry */
  public Duration getFastRetryMaxBackOffPeriod() {
    return fastRetryMaxBackOffPeriod;
  }

  public void setFastRetryMaxBackOffPeriod(Duration fastRetryMaxBackOffPeriod) {
    this.fastRetryMaxBackOffPeriod = fastRetryMaxBackOffPeriod;
  }

  /** Exceptions to retry SLOW */
  public List<Class<? extends Throwable>> getSlowRetryExceptions() {
    return slowRetryExceptions;
  }

  public void setSlowRetryExceptions(List<Class<? extends Throwable>> slowRetryExceptions) {
    this.slowRetryExceptions = slowRetryExceptions;
  }

  /** Max attempts for SLOW retries (including initial attempt) */
  public Integer getSlowRetryMaxAttempts() {
    return slowRetryMaxAttempts;
  }

  public void setSlowRetryMaxAttempts(Integer slowRetryMaxAttempts) {
    this.slowRetryMaxAttempts = slowRetryMaxAttempts;
  }

  /** Interval to wait for the initial SLOW retry */
  public Duration getSlowRetryInitialInterval() {
    return slowRetryInitialInterval;
  }

  public void setSlowRetryInitialInterval(Duration slowRetryInitialInterval) {
    this.slowRetryInitialInterval = slowRetryInitialInterval;
  }

  /** Multiplier applied to the last SLOW trial interval */
  public Double getSlowRetryMultiplier() {
    return slowRetryMultiplier;
  }

  public void setSlowRetryMultiplier(Double slowRetryMultiplier) {
    this.slowRetryMultiplier = slowRetryMultiplier;
  }

  /** BinaryExceptionClassifier created using slowRetryExceptions */
  public BinaryExceptionClassifier getSlowRetryExceptionClassifier() {
    return slowRetryExceptionClassifier;
  }

  /** BinaryExceptionClassifier created using fastRetryExceptions */
  public BinaryExceptionClassifier getFastRetryExceptionClassifier() {
    return fastRetryExceptionClassifier;
  }

  @Override
  public void afterPropertiesSet() {
    this.fastRetryExceptionClassifier = new BinaryExceptionClassifier(this.fastRetryExceptions);
    this.slowRetryExceptionClassifier = new BinaryExceptionClassifier(this.slowRetryExceptions);
  }
}
