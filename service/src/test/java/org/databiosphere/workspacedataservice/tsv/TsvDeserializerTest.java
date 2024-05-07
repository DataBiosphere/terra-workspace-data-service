package org.databiosphere.workspacedataservice.tsv;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.shared.model.attributes.JsonAttribute;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Tests that TSV uploads deserialize into the expected Java objects inside RecordAttributes.
 *
 * @see TsvJsonArgumentsProvider
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest
class TsvDeserializerTest extends TestBase {

  @Autowired TsvDeserializer tsvDeserializer;
  @Autowired ObjectMapper mapper;

  // ===== nodeToObject tests:
  @Test
  void nullNodeInput() {
    JsonNode input = NullNode.getInstance();
    Object actual = tsvDeserializer.nodeToObject(input);
    assertNull(actual, "nodeToObject should return null for NullNode input");
  }

  @Test
  void nonTextNodeInput() {
    JsonNode input = BigIntegerNode.valueOf(BigInteger.valueOf(12345));
    Object actual = tsvDeserializer.nodeToObject(input);
    assertEquals(
        "12345", actual, "nodeToObject should return string representations for non-TextNode");
  }

  @Test
  void textNodeInput() {
    String fixture = "lorem ipsum";
    JsonNode input = TextNode.valueOf(fixture);
    Object actual = tsvDeserializer.nodeToObject(input);
    assertEquals(fixture, actual, "nodeToObject should return value for TextNode");
  }

  // ===== cellToAttribute tests:

  /**
   * @see TsvJsonArgumentsProvider for arguments
   */
  @ParameterizedTest(name = "cellToAttribute for input value <{0}> should return <{1}>")
  @ArgumentsSource(TsvJsonArgumentsProvider.class)
  @ArgumentsSource(TsvOnlyArgumentsProvider.class)
  @MethodSource("jsonInTsvArguments")
  void cellToAttributeTest(String input, Object expected) {
    Object actual = tsvDeserializer.cellToAttribute(input);

    if (expected instanceof List<?> expectedList) {
      assertIterableEquals(
          expectedList,
          (List<?>) actual,
          "cellToAttribute for input value <%s> should return <%s>".formatted(input, expected));
    } else {
      if (Objects.isNull(expected)) {
        assertNull(actual);
      } else {
        assertInstanceOf(expected.getClass(), actual);
      }
      assertEquals(
          expected,
          actual,
          "cellToAttribute for input value <%s> should return <%s>".formatted(input, expected));
    }
  }

  // test cases for deserializing JSON and ARRAY_OF_JSON out of TSV files.
  // these test cases require an ObjectMapper instance, and we want to use WDS's configured
  // ObjectMapper, so this method cannot be static.
  Stream<Arguments> jsonInTsvArguments() throws JsonProcessingException {
    return Stream.of(
        // single json packet
        Arguments.of(
            "{\"foo\":\"bar\", \"baz\": \"qux\"}",
            new JsonAttribute(mapper.readTree("{\"foo\":\"bar\", \"baz\": \"qux\"}"))),
        // array of json packets
        Arguments.of(
            "[{\"value\":\"foo\"},{\"value\":\"bar\"},{\"value\":\"baz\"}]",
            List.of(
                new JsonAttribute(mapper.readTree("{\"value\":\"foo\"}")),
                new JsonAttribute(mapper.readTree("{\"value\":\"bar\"}")),
                new JsonAttribute(mapper.readTree("{\"value\":\"baz\"}")))),
        // nested arrays: numbers
        Arguments.of(
            "[[1],[2,3],[4,5,6]]",
            List.of(
                new JsonAttribute(mapper.readTree("[1]")),
                new JsonAttribute(mapper.readTree("[2,3]")),
                new JsonAttribute(mapper.readTree("[4,5,6]")))),
        // nested arrays: strings
        Arguments.of(
            "[[\"one\"],[\"two\",\"three\"],[\"four\",\"five\",\"six\"]]",
            List.of(
                new JsonAttribute(mapper.readTree("[\"one\"]")),
                new JsonAttribute(mapper.readTree("[\"two\",\"three\"]")),
                new JsonAttribute(mapper.readTree("[\"four\",\"five\",\"six\"]")))),
        // array of mixed json types
        Arguments.of(
            "[[1,2,3],[\"four\",\"five\"],67,{\"some\":\"object\",\"with\":[\"nesting\"]}]",
            List.of(
                new JsonAttribute(mapper.readTree("[1,2,3]")),
                new JsonAttribute(mapper.readTree("[\"four\",\"five\"]")),
                new JsonAttribute(mapper.readTree("67")),
                new JsonAttribute(
                    mapper.readTree("{\"some\":\"object\",\"with\":[\"nesting\"]}")))));
  }
}
