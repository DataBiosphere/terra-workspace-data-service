package org.databiosphere.workspacedataservice;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SentryInitializerTest {

  private static Stream<Arguments> provideSamUrls() {
    /* Arguments are pairs of input Sam URL and expected environment parsed from that URL
     */
    return Stream.of(
        Arguments.of("https://sam.dsde-prod.broadinstitute.org", "prod"),
        Arguments.of("https://sam.dsde-staging.broadinstitute.org", "staging"),
        Arguments.of("https://sam.dsde-alpha.broadinstitute.org", "alpha"),
        Arguments.of("https://sam.dsde-dev.broadinstitute.org", "dev"),
        Arguments.of("https://sam.bee-fancy-generated-name.bee.envs-terra.bio", "unknown"),
        Arguments.of("UNDEFINED", "unknown"),
        Arguments.of("", "unknown"),
        Arguments.of("null", "unknown"),
        Arguments.of(null, "unknown"));
  }

  @ParameterizedTest(
      name = "SentryInitializer.urlToEnv parsing for value [{0}] should result in [{1}]")
  @MethodSource("provideSamUrls")
  void parseProdEnv(String samUrl, String expected) {
    SentryInitializer sentryInitializer = new SentryInitializer();
    String actual = sentryInitializer.urlToEnv(samUrl);
    assertEquals(expected, actual);
  }
}
