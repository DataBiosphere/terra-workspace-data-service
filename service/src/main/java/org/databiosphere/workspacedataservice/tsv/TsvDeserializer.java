package org.databiosphere.workspacedataservice.tsv;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.model.exception.UnexpectedTsvException;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.attributes.JsonAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Jackson deserializer to be used by a CsvMapper. Deserializes the String values produced by
 * the CsvMapper into Objects. The goal of this deserializer is to produce the same classes that
 * JSON deserialization produces.
 */
public class TsvDeserializer extends StdDeserializer<RecordAttributes> {

  public TsvDeserializer(DataTypeInferer inferer, ObjectMapper objectMapper) {
    this(null);
    this.inferer = inferer;
    this.objectMapper = objectMapper;
  }

  public TsvDeserializer(Class<?> vc) {
    super(vc);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TsvDeserializer.class);

  transient DataTypeInferer inferer;
  transient ObjectMapper objectMapper;

  @Override
  public RecordAttributes deserialize(JsonParser parser, DeserializationContext ctxt)
      throws IOException {
    JsonNode node = parser.getCodec().readTree(parser);
    // for TSV deserialization, we expect the row to be an ObjectNode
    if (node.isObject()) {
      Map<String, Object> attrs =
          new HashMap<>(node.size() * 2); // oversize the map to avoid resizing
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        Object attrValue = this.nodeToObject(field.getValue());
        attrs.put(field.getKey(), attrValue);
      }
      return new RecordAttributes(attrs);
    } else {
      LOGGER.warn(
          "TsvDeserializer found root 'row' node of {}; expected json object.",
          node.getClass().getSimpleName());
      return RecordAttributes.empty();
    }
  }

  /**
   * accepts a single cell of a TSV, which is expected to be either a TextNode or a NullNode, and
   * returns o Java object: Boolean, BigInteger, BigDecimal, String, null; or ArrayList containing
   * any of the previous.
   *
   * @param node the JsonNode for the TSV cell
   * @return the deserialized Java object
   */
  public Object nodeToObject(JsonNode node) {
    // TSVs deserialize to Strings
    if (node instanceof TextNode textNode) {
      return cellToAttribute(textNode.textValue());
    }
    if (node instanceof NullNode) {
      return null;
    }
    LOGGER.warn("Unexpected node type {}; returning as text", node.getNodeType());
    return node.asText();
  }

  /**
   * Given the String value representing a TSV cell, return the Java object representing that cell.
   *
   * <p>Note that this method does NOT deserialize dates, datetimes, or relations; that happens
   * elsewhere in WDS code.
   *
   * @param val the String value of the TSV cell
   * @return the deserialized Java object
   */
  public Object cellToAttribute(String val) {
    // nulls
    if (StringUtils.isEmpty(val)) {
      return null;
    }
    /* quoted values: always return as string. This only comes into play when processing array elements;
     * the CSV reader strips surrounding quotes from top-level values.
     */
    if (val.startsWith("\"") && val.endsWith("\"")) {
      return val.substring(1, val.length() - 1);
    }
    // booleans
    if (inferer.isValidBoolean(val)) {
      return Boolean.parseBoolean(val);
    }
    // numbers
    // TODO: change the "is*" methods in inferer to return their value so we don't parse twice?
    if (inferer.isNumericValue(val)) {
      try {
        return new BigInteger(val);
      } catch (NumberFormatException nfe) {
        return new BigDecimal(val);
      }
    }
    // JSON objects
    Optional<ObjectNode> maybeJsonObject = inferer.tryJsonObject(val);
    if (maybeJsonObject.isPresent()) {
      return new JsonAttribute(maybeJsonObject.get());
    }

    // arrays.
    // pseudocode:
    // if this string value looks like an array when lower-cased,
    //    try to parse the array in its original case; use the result if parsing succeeds.
    //    if parsing failed, try again to parse the array as lower-cased; only use this result if it
    // is an array of booleans.
    String smartQuotesRemoved = inferer.replaceLeftRightQuotes(val);
    if (inferer.isArray(smartQuotesRemoved.toLowerCase())) {
      return cellToArray(smartQuotesRemoved);
    }
    return val;
  }

  @SuppressWarnings(
      "java:S1452") // until/unless we strongly type RecordAttributes values, this will be <?>
  public List<?> cellToArray(String val) {
    try {
      return jsonStringToList(val);
    } catch (JsonProcessingException e) {
      // We encountered an error parsing the JSON. This could be due to improperly-cased boolean
      // values.
      // Perform an extra expensive check specifically to parse those booleans.
      // expensive detection of any-cased booleans
      try {
        List<?> lowerElements = jsonStringToList(val.toLowerCase());
        if (lowerElements.stream().allMatch(Boolean.class::isInstance)) {
          return lowerElements;
        }
      } catch (JsonProcessingException innerException) {
        // noop; fall through to the logger/return null just below
      }
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  @SuppressWarnings(
      "java:S1452") // until/unless we strongly type RecordAttributes values, this will be <?>
  public List<?> jsonStringToList(String input) throws JsonProcessingException {
    JsonNode node = objectMapper.readTree(input);
    if (node instanceof ArrayNode arrayNode) {
      // is this an empty array?
      if (arrayNode.isEmpty()) {
        return List.of();
      }

      if (isArrayOfJson(arrayNode)) {
        return elementStream(arrayNode).map(JsonAttribute::new).toList();
      } else {
        return elementStream(arrayNode).map(this::arrayElementToObject).toList();
      }
    } else {
      throw new UnexpectedTsvException(
          "DataTypeInferer.isArray returned true, but the parsed value did not resolve to ArrayNode");
    }
  }

  /**
   * Should this ArrayNode be treated by WDS as ARRAY_OF_JSON?
   *
   * @param arrayNode the input array
   * @return whether WDS should consider this an ARRAY_OF_JSON
   */
  private boolean isArrayOfJson(ArrayNode arrayNode) {
    // AJ-1748: here, we could also detect mixed arrays such as [1,"two",false] and treat those
    // as json instead of stringify-ing them. We can accomplish this by checking the distinct
    // classes of the array elements:
    //     elementStream(arrayNode).map(JsonNode::getClass).distinct().toList();
    // and seeing if they are homogenous. If you do this, be careful of multiple JsonNode
    // subclasses which we treat the same, e.g. IntNode and DecimalNode.

    // is any element of this array itself an array or a json object? This indicates nested
    // structures, so we should treat this as an array of json.
    return elementStream(arrayNode)
        .anyMatch(element -> element instanceof ArrayNode || element instanceof ObjectNode);
  }

  private Stream<JsonNode> elementStream(ArrayNode arrayNode) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(arrayNode.elements(), Spliterator.ORDERED), false);
  }

  /**
   * When a TSV cell contains an array, we parse that array using standard JSON syntax; i.e. TSV
   * cells can contain JSON arrays. This method interprets individual JsonNode array elements and
   * returns the appropriate JSON objects from them.
   *
   * @param element the JSON array element
   * @return deserialized Java Object: Boolean, BigInteger, BigDecimal, String, null
   */
  public Object arrayElementToObject(JsonNode element) {
    if (element instanceof NullNode) {
      return null;
    }
    if (element instanceof NumericNode nn) {
      if (nn.isNaN()) {
        return null;
      }
      try {
        return new BigInteger(nn.numberValue().toString());
      } catch (NumberFormatException nfe) {
        return new BigDecimal(nn.numberValue().toString());
      }
    }
    if (element instanceof BooleanNode bn) {
      return bn.asBoolean();
    }
    if (element instanceof ObjectNode on) {
      return new JsonAttribute(on);
    }
    if (element instanceof TextNode strElement) {
      return cellToAttribute(strElement.toString());
    }
    throw new UnexpectedTsvException(
        "expected an interpretable element, got: " + element.getClass().getName());
  }
}
