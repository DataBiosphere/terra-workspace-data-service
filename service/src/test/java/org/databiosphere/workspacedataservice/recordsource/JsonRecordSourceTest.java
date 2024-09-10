package org.databiosphere.workspacedataservice.recordsource;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class JsonRecordSourceTest extends ControlPlaneTestBase {
  @Autowired ObjectMapper objectMapper; // as defined in JsonConfig

  private static Stream<Arguments> parserFeatures() {
    return Arrays.stream(JsonParser.Feature.values()).map(Arguments::of);
  }

  // test that JsonRecordSource has the same JsonParser features enabled
  // as the main ObjectMapper from JsonConfig
  @ParameterizedTest(name = "JsonParser feature {0} should be same in ObjectMapper and JsonParser")
  @MethodSource("parserFeatures")
  void parserConfig(JsonParser.Feature feature) throws IOException {
    // fake json input needs to start with "[", or creating the JsonRecordSource will fail
    String streamContents = "[]";
    InputStream is = new ByteArrayInputStream(streamContents.getBytes());

    JsonRecordSource handler = new JsonRecordSource(is, objectMapper);

    boolean expected = objectMapper.isEnabled(feature);
    boolean actual = handler.getParser().isEnabled(feature);
    assertEquals(expected, actual, "for feature " + feature.name());
  }
}
