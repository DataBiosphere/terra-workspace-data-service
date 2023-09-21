package bio.terra.common.retry;

import bio.terra.common.db.DatabaseRetryUtils;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.BackOffContext;
import org.springframework.retry.backoff.BackOffInterruptedException;
import org.springframework.retry.backoff.BackOffPolicy;

/**
 * BackOffPolicy that delegates to a number of nested BackOffPolicies. The BackOffPolicy chosen is
 * determined by the exception being handled. If more than one policy matches, the first is used.
 */
public class CompositeBackOffPolicy implements BackOffPolicy {
  private static final Logger logger = LoggerFactory.getLogger(DatabaseRetryUtils.class);

  // LinkedHashMap to ensure consistent behavior when an exception matches more than 1 policy
  private final LinkedHashMap<BinaryExceptionClassifier, BackOffPolicy> backOffPoliciesByClassifier;

  public CompositeBackOffPolicy(
      LinkedHashMap<BinaryExceptionClassifier, BackOffPolicy> backOffPoliciesByClassifier) {
    this.backOffPoliciesByClassifier = backOffPoliciesByClassifier;
  }

  /**
   * This method gets called on entry to a @Retryable function to set up the dynamic context for
   * backing off on a retry. Since we have slow and fast retry, we need to setup context for both
   * and package that inside a composite BackOffContext subclass.
   */
  @Override
  public BackOffContext start(RetryContext context) {
    List<Pair<BinaryExceptionClassifier, BackOffContext>> backOffContexts = new ArrayList<>();
    this.backOffPoliciesByClassifier.forEach(
        (classifier, policy) -> backOffContexts.add(Pair.of(classifier, policy.start(context))));
    return new CompositeBackOffContext(context, backOffContexts);
  }

  @Override
  public void backOff(BackOffContext backOffContext) throws BackOffInterruptedException {
    CompositeBackOffContext compositeBackOffContext = (CompositeBackOffContext) backOffContext;

    for (var classifierAndContext : compositeBackOffContext.backOffContexts) {
      BinaryExceptionClassifier classifier = classifierAndContext.getLeft();
      if (classifier.classify(compositeBackOffContext.retryContext.getLastThrowable())) {
        logger.debug(
            "retrying",
            Map.of(
                "exception",
                compositeBackOffContext.retryContext.getLastThrowable().getClass().getName(),
                "exceptionMessage",
                compositeBackOffContext.retryContext.getLastThrowable().getMessage(),
                "failedTrialCount",
                compositeBackOffContext.retryContext.getRetryCount()));

        backOffPoliciesByClassifier.get(classifier).backOff(classifierAndContext.getRight());
        break; // don't back off for any further matching back off policies
      }
    }
  }

  /**
   * Class to keep track of retry context and a back off context for each nested back off policy.
   */
  private static class CompositeBackOffContext implements BackOffContext {
    private final RetryContext retryContext;
    private final List<Pair<BinaryExceptionClassifier, BackOffContext>> backOffContexts;

    private CompositeBackOffContext(
        RetryContext retryContext,
        List<Pair<BinaryExceptionClassifier, BackOffContext>> backOffContexts) {
      this.retryContext = retryContext;
      this.backOffContexts = backOffContexts;
    }
  }
}
