package org.databiosphere.workspacedataservice.tsv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Service
public class TsvConverter {

    private final DataTypeInferer inferer;

    private final ObjectMapper objectMapper;

    public TsvConverter(DataTypeInferer inferer, ObjectMapper objectMapper) {
        this.inferer = inferer;
        this.objectMapper = objectMapper;
    }

    public Object cellToAttribute(String val) {
        if (Objects.isNull(val)) {
            return null;
        }
        if (inferer.isValidBoolean(val)) {
            return Boolean.parseBoolean(val);
        }
        if (inferer.isNumericValue(val)) {
            return new BigDecimal(val);
        }
        if (inferer.isValidDateTime(val)) {
            return LocalDate.parse(val, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
        if (inferer.isValidDate(val)) {
            return LocalDate.parse(val, DateTimeFormatter.ISO_LOCAL_DATE);
        }
        if (inferer.isValidJson(val)) {
            try {
                // We call .toLowerCase() to ensure that WDS interprets all different inputted spellings of boolean values
                // as booleans - e.g. `TRUE`, `tRUe`, or `true` ---> `true`
                return objectMapper.readTree(val.toLowerCase());
            } catch (JsonProcessingException e) {
                return null;
            }
        }
        if (inferer.isArray(val)) {
            try {
                // We call .toLowerCase() to ensure that WDS interprets all different inputted spellings of boolean values
                // as booleans - e.g. `TRUE`, `tRUe`, or `true` ---> `true`
                JsonNode node = objectMapper.readTree(val.toLowerCase());
                if (node instanceof ArrayNode arrayNode) {

                    Stream<JsonNode> jsonElements = StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(arrayNode.elements(), Spliterator.ORDERED), false);

                    List<Object> typedArray = jsonElements.map( el -> {
                        if (el instanceof NullNode) {
                            return null;
                        }
                        if (el instanceof NumericNode) {
                            if (el.isIntegralNumber()) {
                                return new BigDecimal(el.bigIntegerValue());
                            } else {
                                return new BigDecimal(el.asDouble());
                            }
                        }
                        if (el instanceof BooleanNode) {
                            return el.asBoolean();
                        }
                        if (el instanceof TextNode strElement) {
                            return cellToAttribute(strElement.textValue());
                        }
                        throw new RuntimeException("expected an interpretable element, got: " + el.getClass().getName());
                    }).toList();

                    if (typedArray.stream().map(i -> i.getClass()).distinct().toList().size() > 1) {
                        throw new RuntimeException("found mixed array!");
                    }

                    return typedArray;
                } else {
                    throw new RuntimeException("array but not array");
                }
            } catch (JsonProcessingException e) {
                return null;
            }
        }
        return val;
    }

    public RecordAttributes fromTsvRow(Map<String, String> row) {
        Map<String, Object> typedAttrs = row.entrySet().stream().map( entry -> {
            return new AbstractMap.SimpleEntry<>(entry.getKey(), cellToAttribute(entry.getValue()));
        })
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new RecordAttributes(typedAttrs);
    }

    public Record tsvRowToRecord(Map<String, String> row, RecordType recordType, String primaryKey) {
        // TODO: friendlier error if pk doesn't exist in map
        String recordId = row.get(primaryKey).toString();
        RecordAttributes typedAttrs = fromTsvRow(row);
        return new Record(recordId, recordType, typedAttrs);
    }


}
