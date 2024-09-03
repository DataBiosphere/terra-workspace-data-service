package org.databiosphere.workspacedataservice.sam;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.context.request.RequestContextHolder;

/** Tests for @see BearerTokenFilter */
@SpringBootTest
class BearerTokenFilterTest extends DataPlaneTestBase {

  private static Stream<Arguments> provideAuthorizationHeaders() {
    /* Arguments are sets:
    - first value is the Authorization header in the incoming request
    - second value is the expected value that BearerTokenFilter will extract and save
    */
    return Stream.of(
        // does not start with "Bearer " prefix (note space after Bearer)
        Arguments.of("not a bearer token", null),
        Arguments.of("Bearer", null),
        Arguments.of("Bearer-no-space-delimiter", null),
        Arguments.of("something something Bearer something", null),
        Arguments.of("bearer lower-case", null),
        // starts properly, but no value after the prefix
        Arguments.of("Bearer ", ""),
        // valid tokens
        Arguments.of("Bearer mytoken", "mytoken"),
        Arguments.of("Bearer !#$%..^", "!#$%..^"));
  }

  /**
   * Verifies that BearerTokenFilter will: - find an Authorization header in the incoming request -
   * extract a syntactically-correct Bearer token from the header - save the token value into the
   * RequestContextHolder
   *
   * @param input value in Authorization request header
   * @param expected expected token extracted and saved by BearerTokenFilter
   * @throws Exception exception
   */
  @ParameterizedTest(
      name = "Request Authorization header of <{0}> should result in a token of <{1}>")
  @MethodSource("provideAuthorizationHeaders")
  void extractTokenTest(String input, String expected) throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.addHeader(HttpHeaders.AUTHORIZATION, input);

    MockHttpServletResponse response = new MockHttpServletResponse();
    MockFilterChain filterChain = new MockFilterChain();

    new BearerTokenFilter().doFilter(request, response, filterChain);

    Object actual =
        RequestContextHolder.currentRequestAttributes()
            .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);

    assertEquals(expected, actual);
  }

  // long token could be included in the extractTokenTest() parameters, but generates an unwieldy
  // test name
  @Test
  void extractLongTokenTest() throws Exception {
    String longToken = RandomStringUtils.randomAscii(8192);

    MockHttpServletRequest request = new MockHttpServletRequest();
    request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + longToken);

    MockHttpServletResponse response = new MockHttpServletResponse();
    MockFilterChain filterChain = new MockFilterChain();

    new BearerTokenFilter().doFilter(request, response, filterChain);

    Object actual =
        RequestContextHolder.currentRequestAttributes()
            .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);

    assertEquals(longToken, actual);
  }

  /**
   * Tests that a request with no Authorization: header still works but results in no token saved to
   * RequestContextHolder
   *
   * @throws Exception exception
   */
  @Test
  void noAuthorizationHeaderTest() throws Exception {
    MockHttpServletRequest request = new MockHttpServletRequest();
    // note: no Authorization header on the request object

    MockHttpServletResponse response = new MockHttpServletResponse();
    MockFilterChain filterChain = new MockFilterChain();

    new BearerTokenFilter().doFilter(request, response, filterChain);

    Object actual =
        RequestContextHolder.currentRequestAttributes()
            .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);

    assertNull(actual);
  }
}
