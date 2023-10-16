package org.databiosphere.workspacedataservice.jobexec;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Modeled after Spring's RequestContextHolder, this class manages a ThreadLocal Map<String, Object>
 * so callers can stash values onto the thread, then later retrieve those values from the thread.
 *
 * <p>Callers MUST call JobContextHolder.destroy() when they are done with the underlying
 * ThreadLocal. This should be done in a `finally` to ensure it always gets called. If callers do
 * not reliably call destroy(), it can lead to threads receiving dangling data they should not have
 * access to, as well as memory leaks.
 *
 * <p>Callers must also call init() before setting any values; this helps safeguard against dangling
 * data.
 */
public class JobContextHolder {
  private static final ThreadLocal<Map<String, Object>> JOB_CONTEXT = new ThreadLocal<>();

  private JobContextHolder() {}

  /** initialize this thread's usage of JobContextHolder */
  public static void init() {
    JOB_CONTEXT.set(new ConcurrentHashMap<>());
  }

  /**
   * Retrieve the entire map of attributes from this context.
   *
   * @return all attributes
   */
  public static Map<String, Object> getAttributes() {
    return JOB_CONTEXT.get();
  }

  /**
   * Put a single value into the attributes for this context.
   *
   * @param key attribute name
   * @param value attribute value
   */
  public static void setAttribute(String key, Object value) {
    Map<String, Object> curr = getAttributes();
    if (curr != null) {
      curr.put(key, value);
    }
  }

  /**
   * Retrieve a single value from this context's attributes.
   *
   * @param key attribute name
   * @return the attribute value, or null if the key was not found or init() has not yet been called
   */
  public static Object getAttribute(String key) {
    Map<String, Object> curr = getAttributes();
    if (curr == null) {
      return null;
    }
    return curr.get(key);
  }

  /** Clean up by removing all attributes from this context. */
  public static void destroy() {
    JOB_CONTEXT.remove();
  }
}
