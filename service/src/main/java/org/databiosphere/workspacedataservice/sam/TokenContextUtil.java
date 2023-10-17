package org.databiosphere.workspacedataservice.sam;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

import java.util.Map;
import java.util.function.Supplier;
import org.databiosphere.workspacedataservice.jobexec.JobContextHolder;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
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
   * <p>If not found in any of the above, return null.
   *
   * @param initialValue the first value to check for a non-null token
   * @return the final value
   */
  public static BearerToken getToken(String initialValue) {
    return getToken(new BearerToken(initialValue), () -> null);
  }

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
  public static BearerToken getToken(BearerToken initialValue, Supplier<BearerToken> orElse) {
    if (initialValue != null && initialValue.value() != null) {
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
  public static BearerToken getToken(Supplier<BearerToken> orElse) {
    BearerToken foundToken = getToken();
    if (foundToken != null) {
      // N.B. no logging here; this is the simplest case
      return foundToken;
    } else {
      LOGGER.debug("Token defaulted from orElse supplier.");
      return orElse.get();
    }
  }

  /**
   * Look in RequestContextHolder and then JobContextHolder, in that order, for a non-null String
   * ATTRIBUTE_NAME_TOKEN
   *
   * @return the token if found; null otherwise
   */
  public static BearerToken getToken() {
    BearerToken foundToken;
    // look in request context; if non-null, return it
    foundToken = tokenFromRequestContext();
    if (foundToken != null) {
      LOGGER.debug("Token retrieved from request context.");
      return foundToken;
    }

    // look in job context; return whatever we found, even if null
    foundToken = tokenFromJobContext();
    if (foundToken != null) {
      LOGGER.debug("Token retrieved from job context.");
    }
    return foundToken;
  }

  /**
   * Look in RequestContextHolder for a non-null String ATTRIBUTE_NAME_TOKEN
   *
   * @return the token if found; null otherwise
   */
  private static BearerToken tokenFromRequestContext() {
    // do any request attributes exist?
    RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
    if (requestAttributes != null) {
      return maybeToken(requestAttributes.getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST));
    }
    return null;
  }

  /**
   * Look in JobContextHolder for a non-null String ATTRIBUTE_NAME_TOKEN
   *
   * @return the token if found; null otherwise
   */
  private static BearerToken tokenFromJobContext() {
    // do any job attributes exist?
    Map<String, Object> jobAttributes = JobContextHolder.getAttributes();
    if (jobAttributes == null) {
      return null;
    }
    return maybeToken(jobAttributes.get(ATTRIBUTE_NAME_TOKEN));
  }

  /** Convenience: is the input object non-null and a String? */
  private static BearerToken maybeToken(Object obj) {
    // as of this writing, if "obj instanceof String" passes, then "BearerToken.isValid" will always
    // pass. The check is included here for future compatibility, in case we change the isValid
    // implementation later.
    if (obj instanceof String strVal && BearerToken.isValid(strVal)) {
      return new BearerToken(strVal);
    }
    return null;
  }
}
