package org.databiosphere.workspacedataservice.sam;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

import java.util.Map;
import java.util.function.Supplier;
import org.databiosphere.workspacedataservice.jobexec.JobContextHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

public class TokenContextUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(TokenContextUtil.class);

  private TokenContextUtil() {}

  /**
   * Convenience for evaluating auth tokens.
   *
   * <p>If `initialValue` is non-null, returns the initialValue as-is.
   *
   * <p>If `initialValue` is null, searches request context for a non-null String value, and returns
   * it if found.
   *
   * <p>If the request context has no value, searches job context for a non-null String value, and
   * returns it if found.
   *
   * <p>If the job context has no value, returns the value of the `orElse` supplier.
   *
   * @param initialValue the first value to check for a non-null token
   * @param orElse the value to return if the token was not found otherwise
   * @return the final value
   */
  public static String getToken(String initialValue, Supplier<String> orElse) {
    if (initialValue != null) {
      return initialValue;
    }
    return getToken(orElse);
  }

  /**
   * Look in RequestContextHolder and then JobContextHolder, in that order, for a non-null String
   * ATTRIBUTE_NAME_TOKEN. If none found, return the value of orElse.
   *
   * @param orElse the value to return if the token was not found otherwise
   * @return the final value
   */
  public static String getToken(Supplier<String> orElse) {
    String foundToken = getToken();
    if (foundToken != null) {
      return foundToken;
    } else {
      return orElse.get();
    }
  }

  /**
   * Look in RequestContextHolder and then JobContextHolder, in that order, for a non-null String
   * ATTRIBUTE_NAME_TOKEN
   *
   * @return the token if found; null otherwise
   */
  public static String getToken() {
    String foundToken;
    // look in request context; if non-null, return it
    foundToken = tokenFromRequestContext();
    if (foundToken != null) {
      return foundToken;
    }

    // look in job context; return whatever we found, even if null
    return tokenFromJobContext();
  }

  /**
   * Look in RequestContextHolder for a non-null String ATTRIBUTE_NAME_TOKEN
   *
   * @return the token if found; null otherwise
   */
  private static String tokenFromRequestContext() {
    // do any request attributes exist?
    RequestAttributes requestAttributes;
    try {
      requestAttributes = RequestContextHolder.currentRequestAttributes();
    } catch (IllegalStateException e) {
      LOGGER.debug("No request attributes on this thread.");
      return null;
    }
    return maybeToken(requestAttributes.getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST));
  }

  /**
   * Look in JobContextHolder for a non-null String ATTRIBUTE_NAME_TOKEN
   *
   * @return the token if found; null otherwise
   */
  private static String tokenFromJobContext() {
    // do any job attributes exist?
    Map<String, Object> jobAttributes = JobContextHolder.getAttributes();
    if (jobAttributes == null) {
      LOGGER.debug("No job attributes on this thread.");
      return null;
    }

    return maybeToken(jobAttributes.get(ATTRIBUTE_NAME_TOKEN));
  }

  /** Convenience: is the input object non-null and a String? */
  private static String maybeToken(Object obj) {
    if (obj instanceof String strVal) {
      return strVal;
    }
    return null;
  }
}
