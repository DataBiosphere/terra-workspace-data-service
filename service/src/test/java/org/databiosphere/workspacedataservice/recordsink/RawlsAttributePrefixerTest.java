package org.databiosphere.workspacedataservice.recordsink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RawlsAttributePrefixerTest {

  private static final String RECTYPE = "rectype";

  // test cases for TDR prefix strategy, in the form of input, expected
  static Stream<Arguments> tdrTestCases() {
    return Stream.of(
        Arguments.of("name", "tdr:name"),
        Arguments.of("entityType", "tdr:entityType"),
        Arguments.of(RECTYPE + "_id", "tdr:" + RECTYPE + "_id"),
        Arguments.of("notrectype_id", "notrectype_id"),
        Arguments.of("someattr", "someattr"),
        Arguments.of("name_and_other_stuff", "name_and_other_stuff"));
  }

  // test cases for PFB prefix strategy, in the form of input, expected
  static Stream<Arguments> pfbTestCases() {
    return Stream.of(
        Arguments.of("name", "pfb:" + RECTYPE + "_name"),
        Arguments.of("entityType", "pfb:entityType"),
        Arguments.of(RECTYPE + "_id", "pfb:" + RECTYPE + "_id"),
        Arguments.of("notrectype_id", "pfb:notrectype_id"),
        Arguments.of("someattr", "pfb:someattr"),
        Arguments.of("name_and_other_stuff", "pfb:name_and_other_stuff"));
  }

  @ParameterizedTest(name = "RawlsAttributePrefixer(TDR) should prefix {0} to {1}")
  @MethodSource("tdrTestCases")
  void tdrPrefixing(String input, String expected) {
    RawlsAttributePrefixer prefixer =
        new RawlsAttributePrefixer(RawlsAttributePrefixer.PrefixStrategy.TDR);
    assertEquals(expected, prefixer.prefix(input, RECTYPE));
  }

  @ParameterizedTest(name = "RawlsAttributePrefixer(PFB) should prefix {0} to {1}")
  @MethodSource("pfbTestCases")
  void pfbPrefixing(String input, String expected) {
    RawlsAttributePrefixer prefixer =
        new RawlsAttributePrefixer(RawlsAttributePrefixer.PrefixStrategy.PFB);
    assertEquals(expected, prefixer.prefix(input, RECTYPE));
  }
}
