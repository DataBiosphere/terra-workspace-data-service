package org.databiosphere.workspacedataservice;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SentryInitializerTest {

  private static Stream<Arguments> provideProfiles() {
    /* Arguments are pairs of input Sam URL and expected environment parsed from that URL
     */
    return Stream.of(
        Arguments.of("prod, data-plane", true),
        Arguments.of("staging, data-plane", true),
        Arguments.of("dev, data-plane", true),
        Arguments.of("local, data-plane", false),
        Arguments.of("bee, data-plane", false),
        Arguments.of("UNDEFINED", false),
        Arguments.of("", false),
        Arguments.of("null", false));
  }

  @ParameterizedTest(
      name = "SentryInitializer.urlToEnv parsing for value [{0}] should result in [{1}]")
  @MethodSource("provideProfiles")
  void parseProdEnv(String profiles, Boolean expected) {
    SentryInitializer sentryInitializer = new SentryInitializer();
    boolean actual = sentryInitializer.determineIfEnvIsMonitored(profiles);
    assertEquals(expected, actual);
  }
}
