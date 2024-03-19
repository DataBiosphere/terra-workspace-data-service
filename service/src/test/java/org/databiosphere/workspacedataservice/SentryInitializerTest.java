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
        Arguments.of(new String[]{"prod, data-plane"}, "prod"),
        Arguments.of(new String[]{"staging, data-plane"}, "staging"),
        Arguments.of(new String[]{"dev, data-plane"}, "dev"),
        Arguments.of(new String[]{"local, data-plane"}, ""),
        Arguments.of(new String[]{"bee, data-plane"}, ""),
        Arguments.of(new String[]{"UNDEFINED"}, ""),
        Arguments.of(new String[]{""}, ""),
        Arguments.of(new String[]{"null"}, ""));
  }

  @ParameterizedTest(
      name = "SentryInitializer.urlToEnv parsing for value [{0}] should result in [{1}]")
  @MethodSource("provideProfiles")
  void parseProdEnv(String[] profiles, Boolean expected) {
    SentryInitializer sentryInitializer = new SentryInitializer();
    String actual = sentryInitializer.getSentryEnvironment(profiles);
    assertEquals(expected, actual);
  }
}
