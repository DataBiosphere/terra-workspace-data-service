package org.databiosphere.workspacedataservice.tsv;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.*;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Custom Jackson deserializer to be used by a CsvMapper. Deserializes
 * the String values produced by the CsvMapper into Objects. The goal of this
 * deserializer is to produce the same classes that JSON deserialization produces.
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

    DataTypeInferer inferer;
    ObjectMapper objectMapper;

    @Override
    public RecordAttributes deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
        JsonNode node = parser.getCodec().readTree(parser);
        // for TSV deserialization, we expect the row to be an ObjectNode
        if (node.isObject()) {
            Map<String, Object> attrs = new HashMap<>(node.size() * 2); // oversize the map to avoid resizing
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                Object attrValue = this.nodeToObject(field.getValue());
                attrs.put(field.getKey(), attrValue);
            }
            return new RecordAttributes(attrs);
        } else {
            LOGGER.warn("TsvDeserializer found root 'row' node of {}; expected json object.", node.getClass().getSimpleName());
            return RecordAttributes.empty();
        }
    }

    /**
     * accepts a single cell of a TSV, which is expected to be either a TextNode or a NullNode,
     * and returns o Java object: Boolean, BigInteger, BigDecimal, String, null; or ArrayList containing any
     * of the previous.
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
     * <p/>
     * Note that this method does NOT deserialize dates, datetimes, or relations; that happens elsewhere
     * in WDS code.
     *
     * @param val the String value of the TSV cell
     * @return the deserialized Java object
     */
    public Object cellToAttribute(String val) {
        // nulls
        if (Objects.isNull(val) || StringUtils.isEmpty(val)) {
            return null;
        }
        // quoted values: always return as string. This only comes into play when processing array elements;
        // the CSV reader strips surrounding quotes from top-level values.
        if (val.startsWith("\"") && val.endsWith("\"")) {
            return val.substring(1, val.length()-1);
        }
        // booleans
        // TODO: change the "is*" methods in inferer to return their value so we don't parse twice?
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
        // TODO: change the "is*" methods in inferer to return their value so we don't parse twice?
        if (inferer.isValidJson(val)) {
            try {
                return objectMapper.readValue(val, new TypeReference<Map<String, Object>>(){});
            } catch (JsonProcessingException jpe) {
                // this shouldn't happen; if inferer.isValidJson(val) passes, so should the .readValue
                return val;
            }
        }

        // arrays
        String smartQuotesRemoved = replaceLeftRightQuotes(val);
        if (inferer.isArray(smartQuotesRemoved.toLowerCase())) {
            try {
                return jsonStringToList(smartQuotesRemoved);
            } catch (JsonProcessingException e) {
                // We encountered an error parsing the JSON. This could be due to improperly-cased boolean values.
                // Perform an extra expensive check specifically to parse those booleans.
                // expensive detection of any-cased booleans
                try {
                    List<?> lowerElements = jsonStringToList(smartQuotesRemoved.toLowerCase());
                    if (lowerElements.stream().allMatch(element -> element instanceof Boolean)) {
                        return lowerElements;
                    }
                } catch (JsonProcessingException innerException) {
                    // noop; fall through to the logger/return null just below
                }
                LOGGER.error(e.getMessage(), e);
                return null;
            }
        }
        return val;
    }

    public List<?> jsonStringToList(String input) throws JsonProcessingException {
        JsonNode node = objectMapper.readTree(input);
        if (node instanceof ArrayNode arrayNode) {
            Stream<JsonNode> jsonElements = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(arrayNode.elements(), Spliterator.ORDERED), false);

            return jsonElements.map( el -> arrayElementToObject(el)).toList();
        } else {
            throw new RuntimeException("DataTypeInferer.isArray returned true, but the parsed value did not resolve to ArrayNode");
        }
    }

    /**
     * When a TSV cell contains an array, we parse that array using standard JSON syntax; i.e.
     * TSV cells can contain JSON arrays. This method interprets individual JsonNode array elements
     * and returns the appropriate JSON objects from them.
     * @param element the JSON array element
     * @return deserialized Java Object: Boolean, BigInteger, BigDecimal, String, null
     */
    public Object arrayElementToObject(JsonNode element) {
        if (element instanceof NullNode) {
            return null;
        }
        if (element instanceof NumericNode nn) {
            try {
                return new BigInteger(nn.numberValue().toString());
            } catch (NumberFormatException nfe) {
                return new BigDecimal(nn.numberValue().toString());
            }
        }
        if (element instanceof BooleanNode bn) {
            return bn.asBoolean();
        }
        if (element instanceof TextNode strElement) {
            return cellToAttribute(strElement.toString());
        }
        throw new RuntimeException("expected an interpretable element, got: " + element.getClass().getName());
    }

    // TODO: this is copied from DataTypeInferer; don't copy!
    private String replaceLeftRightQuotes(String val){
        return val.replaceAll("[“”]", "\"");
    }

}
