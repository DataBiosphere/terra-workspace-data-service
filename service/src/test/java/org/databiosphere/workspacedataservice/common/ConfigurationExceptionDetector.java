package org.databiosphere.workspacedataservice.common;

import org.databiosphere.workspacedataservice.config.ConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.opentest4j.AssertionFailedError;

/** Detects unhandled ConfigurationExceptions and provides a resolution hint. */
public class ConfigurationExceptionDetector implements TestWatcher {

  private static final String RESOLUTION_HINT =
      "You may need to specify @ActiveProfiles with \"data-plane\" or \"control-plane\" or extend %s"
          .formatted(DataPlaneTestBase.class.getSimpleName());

  @Override
  public void testFailed(ExtensionContext context, Throwable cause) {
    // Check if the cause or any cause in the chain is the specific RuntimeException
    Throwable rootCause = getRootCause(cause);
    if (rootCause instanceof ConfigurationException) {
      throw new AssertionError(
          "Test failed due to an unhandled ConfigurationException! " + RESOLUTION_HINT, rootCause);
    }

    if (rootCause instanceof AssertionFailedError afe && isUnexpectedConfigurationException(afe)) {
      throw new AssertionError(
          "Test failed due to an unexpected ConfigurationException! " + RESOLUTION_HINT, rootCause);
    }
  }

  private boolean isUnexpectedConfigurationException(AssertionFailedError afe) {
    boolean actualValueIsConfigurationException =
        afe.getActual() != null && afe.getActual().getType().equals(ConfigurationException.class);

    boolean expectedValueIsNot =
        afe.getExpected() == null
            || !ConfigurationException.class.equals(afe.getExpected().getType());

    return actualValueIsConfigurationException && expectedValueIsNot;
  }

  private Throwable getRootCause(Throwable throwable) {
    Throwable cause = throwable.getCause();
    return (cause == null || cause == throwable) ? throwable : getRootCause(cause);
  }
}
