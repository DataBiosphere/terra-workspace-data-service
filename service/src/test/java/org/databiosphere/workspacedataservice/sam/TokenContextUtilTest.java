package org.databiosphere.workspacedataservice.sam;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.jobexec.JobContextHolder;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

public class TokenContextUtilTest {

  @Test
  void nonNullInitialValue() {
    String expected = RandomStringUtils.randomAlphanumeric(10);

    // set a dummy value into request attributes
    MockHttpServletRequest request = new MockHttpServletRequest();
    RequestAttributes requestAttributes = new ServletRequestAttributes(request);
    requestAttributes.setAttribute(ATTRIBUTE_NAME_TOKEN, "dummy request value", SCOPE_REQUEST);
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));

    try {
      // and set a dummy value into job attributes
      JobContextHolder.init();
      JobContextHolder.setAttribute(ATTRIBUTE_NAME_TOKEN, "dummy job value");

      // call getToken with a non-null initialValue
      String actual = TokenContextUtil.getToken(expected, () -> "dummy orElse value");
      assertEquals(expected, actual);
    } finally {
      JobContextHolder.destroy();
    }
  }

  @Test
  void valueInRequest() {
    String expected = RandomStringUtils.randomAlphanumeric(10);

    // set the expected token into request attributes
    MockHttpServletRequest request = new MockHttpServletRequest();
    RequestAttributes requestAttributes = new ServletRequestAttributes(request);
    requestAttributes.setAttribute(ATTRIBUTE_NAME_TOKEN, expected, SCOPE_REQUEST);
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));

    try {
      // and set a dummy value into job attributes
      JobContextHolder.init();
      JobContextHolder.setAttribute(ATTRIBUTE_NAME_TOKEN, "dummy job value");

      // call getToken with a null initialValue
      String actual = TokenContextUtil.getToken(null, () -> "dummy orElse value");
      assertEquals(expected, actual);
    } finally {
      JobContextHolder.destroy();
    }
  }

  @Test
  void valueInJob() {
    String expected = RandomStringUtils.randomAlphanumeric(10);

    // set request attributes to empty
    RequestContextHolder.setRequestAttributes(null);

    try {
      // set the expected token into job attributes
      JobContextHolder.init();
      JobContextHolder.setAttribute(ATTRIBUTE_NAME_TOKEN, expected);
      // call getToken with a null initialValue
      String actual = TokenContextUtil.getToken(null, () -> "dummy orElse value");
      assertEquals(expected, actual);
    } finally {
      JobContextHolder.destroy();
    }
  }

  @Test
  void callOrElse() {
    String expected = RandomStringUtils.randomAlphanumeric(10);
    // set request attributes to empty
    RequestContextHolder.setRequestAttributes(null);
    // ensure job attributes are empty
    JobContextHolder.destroy();
    // call getToken with a null initialValue and an orElse that returns the expected value
    String actual = TokenContextUtil.getToken(null, () -> expected);
    assertEquals(expected, actual);
  }

  @Test
  void throwInOrElse() {
    // set request attributes to empty
    RequestContextHolder.setRequestAttributes(null);
    // ensure job attributes are empty
    JobContextHolder.destroy();

    // call getToken with a null initialValue and an orElse that throws
    AuthenticationException authenticationException =
        assertThrows(
            AuthenticationException.class,
            () ->
                TokenContextUtil.getToken(
                    null,
                    () -> {
                      throw new AuthenticationException("unit testing!");
                    }));

    assertEquals("401 UNAUTHORIZED \"unit testing!\"", authenticationException.getMessage());
  }
}
