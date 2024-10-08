package org.databiosphere.workspacedataservice.service;

import jakarta.servlet.ServletRequestEvent;
import jakarta.servlet.ServletRequestListener;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.hashids.Hashids;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

/**
 * Servlet request listener that sets a unique request id into the SLF4J MDC context for this
 * request. This listener does this by looking in the incoming request headers for an
 * "x-b3-traceid", "x-request-id", or "trace-id" value (in that order) and using the first 64
 * characters of that value if present.
 *
 * <p>If no id is present in the incoming request headers, it will assign a new UUID value for the
 * request id.
 *
 * <p>See also {@link MDCResponseHeaderFilter}, which adds the "requestId" value to the MDC context.
 */
@Component
public class MDCServletRequestListener implements ServletRequestListener {

  // where the unique request id is stored in the MDC context
  public static final String MDC_KEY = "requestId";
  // the response header containing the unique request id
  public static final String RESPONSE_HEADER = "x-b3-traceid";
  // the list of request headers, in order, we will search for an incoming unique request id
  // we could add other headers here, such as: x-vcap-request-id, X─B3─ParentSpanId, X─B3─SpanId,
  // X─B3─Sampled, x-amzn-trace-id
  static final List<String> INCOMING_HEADERS =
      Arrays.asList(RESPONSE_HEADER, "x-request-id", "trace-id");

  private final Hashids hashids = new Hashids(MDCServletRequestListener.class.getName(), 8);

  @Override
  public void requestInitialized(ServletRequestEvent requestEvent) {
    String uniqueId = null;
    // look for a non-blank unique request id in the incoming request headers
    if (requestEvent.getServletRequest() instanceof HttpServletRequest httpServletRequest) {
      for (String hdr : INCOMING_HEADERS) {
        String foundIncoming = httpServletRequest.getHeader(hdr);
        if (foundIncoming != null && !foundIncoming.isBlank()) {
          uniqueId = foundIncoming.length() > 64 ? foundIncoming.substring(0, 64) : foundIncoming;
          break;
        }
      }
    }

    // if we did not find an id in incoming headers, make a unique one
    if (uniqueId == null) {
      uniqueId = generateId();
    }

    // add id to MDC logging
    MDC.put(MDC_KEY, uniqueId);
  }

  /*
     Pseudo-random ID generation is fine here. These ids are meant to uniquely identify a
     single request. They are not used for cryptography or anything security-sensitive.
  */
  @SuppressWarnings("java:S2245")
  private String generateId() {
    return hashids.encode(ThreadLocalRandom.current().nextLong(0, Integer.MAX_VALUE));
  }

  @Override
  public void requestDestroyed(ServletRequestEvent requestEvent) {
    MDC.clear();
  }
}
