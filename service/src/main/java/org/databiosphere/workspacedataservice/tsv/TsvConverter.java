package org.databiosphere.workspacedataservice.tsv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidTsvException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Service
public class TsvConverter {

    private final DataTypeInferer inferer;

    private final ObjectMapper objectMapper;

    private static final Logger LOGGER = LoggerFactory.getLogger(TsvConverter.class);

    public TsvConverter(DataTypeInferer inferer, ObjectMapper objectMapper) {
        this.inferer = inferer;
        this.objectMapper = objectMapper;
    }

    private String replaceLeftRightQuotes(String val){
        return val.replaceAll("[“”]", "\"");
    }

    public Object cellToAttribute(String val) {
        /*
        TODO need to handle:
            - numbers
            - booleans
            - json objects
            - arrays of any of the previous
            - quoted values
        do not need to handle:
            - dates, datetimes, relations; these are detected later in the processing pipeline
         */

        LOGGER.info("***** cellToAttribute: <" + val + ">");
        if (Objects.isNull(val)) {
            return null;
        }
        // anything quoted should be a string. This only comes into play when processing array elements;
        // the CSV reader strips surrounding quotes from top-level values.
        if (val.startsWith("\"") && val.endsWith("\"")) {
            String trimmed = val.substring(1, val.length()-1);
            LOGGER.info("***** for input <" + val + ">, returning trimmed string: " + trimmed);
            return trimmed;
        }
        // booleans and numbers. TODO: change the "is*" methods in inferer to return their value so we don't parse twice?
        if (inferer.isValidBoolean(val)) {
            return Boolean.parseBoolean(val);
        }
        if (inferer.isNumericValue(val)) {
            try {
                return new BigInteger(val);
            } catch (NumberFormatException nfe) {
                return new BigDecimal(val);
            }
        }

        if (inferer.isValidJson(val)) {
            try {
                return objectMapper.readValue(val, new TypeReference<Map<String, Object>>(){});
            } catch (JsonProcessingException jpe) {
                // this shouldn't happen; if inferer.isValidJson(val) passes, so should the .readValue
                return val;
            }
        }

        // date/datetime detection happens later in the pipeline; we just need to deserialize to strings
        if (inferer.isArray(replaceLeftRightQuotes(val))) {
            LOGGER.info("***** for input <" + val + ">, processing as array");
            try {
                // We call .toLowerCase() to ensure that WDS interprets all different inputted spellings of boolean values
                // as booleans - e.g. `TRUE`, `tRUe`, or `true` ---> `true`
                // TODO: don't automatically lower-case this!
                JsonNode node = objectMapper.readTree(replaceLeftRightQuotes(val.toLowerCase()));
                LOGGER.info("***** for input <" + val + ">, parsed is array? " + node.isArray());
                if (node instanceof ArrayNode arrayNode) {

                    Stream<JsonNode> jsonElements = StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(arrayNode.elements(), Spliterator.ORDERED), false);

                    List<Object> typedArray = jsonElements.map( el -> {
                        if (el instanceof NullNode) {
                            LOGGER.info("***** for array element <" + el + ">, returning null");
                            return null;
                        }
                        if (el instanceof NumericNode nn) {

                            try {
                                BigInteger bigInteger = new BigInteger(nn.numberValue().toString());
                                LOGGER.info("***** for array element <" + el + ">, returning BigInteger");
                                return bigInteger;
                            } catch (NumberFormatException nfe) {
                                BigDecimal bigDecimal = new BigDecimal(nn.numberValue().toString());
                                LOGGER.info("***** for array element <" + el + ">, returning BigDecimal");
                                return bigDecimal;
                            }
                        }
                        if (el instanceof BooleanNode bn) {
                            LOGGER.info("***** for array element <" + el + ">, returning boolean");
                            return bn.asBoolean();
                        }
                        if (el instanceof TextNode strElement) {
                            LOGGER.info("***** for array element <" + el + ">, delegating to cellToAttribute");
                            return cellToAttribute(strElement.toString());
                        }
                        throw new RuntimeException("expected an interpretable element, got: " + el.getClass().getName());
                    }).toList();

                    if (typedArray.size() > 0) {
                        LOGGER.info("***** for input <" + val + ">, typedArray contains: " + typedArray.get(0).getClass().getName());
                    } else {
                        LOGGER.info("***** for input <" + val + ">, typedArray is empty array");
                    }



                    List<String> classNames = typedArray.stream().map(i -> i.getClass().getName()).distinct().toList();

                    // special case for BigDecimal/BigInteger; TODO: make this cleaner
                    if (classNames.size() > 1 && !(classNames.size() == 2 && classNames.contains(BigDecimal.class.getName()) && classNames.contains(BigInteger.class.getName()))) {

                        // TODO: call 'em strings. This could be more graceful.
                        Stream<JsonNode> forceStringElements = StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(arrayNode.elements(), Spliterator.ORDERED), false);

                        LOGGER.info("***** for input <" + val + ">, forcing to strings: " + typedArray);

                        List<String> forcedStrings = forceStringElements.map(el -> {
                            String tv = el.textValue();
                            if (Objects.isNull(tv)) {
                                return el.toString();
                            } else {
                                return tv;
                            }
                        }).toList();
                        if (forcedStrings.size() > 0) {
                            LOGGER.info("***** for input <" + val + ">, forcing to strings: " + forcedStrings);
                            return forcedStrings;
                        } else {
                            return null;
                        }
                    }

                    return typedArray;

                } else {
                    throw new RuntimeException("array but not array");
                }
            } catch (JsonProcessingException e) {
                LOGGER.info("***** for input <" + val + ">, hit error: " + e.getMessage());
                return null;
            }
        }

        LOGGER.info("***** for input <" + val + ">, defaulting to no translation and returning string.");

        return val;
    }

    public RecordAttributes fromTsvRow(Map<String, String> row) {
        List<AbstractMap.SimpleEntry<String, Object>> typedAttrs = row.entrySet().stream().map(entry ->
                        new AbstractMap.SimpleEntry<>(entry.getKey(), cellToAttribute(entry.getValue()))).toList();
        // can't use a collector here because some values can be null
        Map<String, Object> attrMap = new HashMap<String, Object>();
        typedAttrs.forEach( entry -> attrMap.put(entry.getKey(), entry.getValue()));
        return new RecordAttributes(attrMap);
    }

    public Record tsvRowToRecord(Map<String, String> row, RecordType recordType, String primaryKey) {
        Object recordId = row.get(primaryKey);
        if (recordId == null || StringUtils.isBlank(recordId.toString())) {
            throw new InvalidTsvException(
                    "Uploaded TSV is either missing the " + primaryKey
                            + " column or has a null or empty string value in that column");
        }
        row.remove(primaryKey);
        RecordAttributes typedAttrs = fromTsvRow(row);
        return new Record(recordId.toString(), recordType, typedAttrs);
    }

    public Stream<Record> rowsToRecords(Stream<Map<String, String>> rows, RecordType recordType, String primaryKey) {
        HashSet<String> recordIds = new HashSet<>(); // this set may be slow for very large TSVs
        return rows.map( m -> {
            Record r = tsvRowToRecord(m, recordType, primaryKey);
            if (!recordIds.add(r.getId())) {
                throw new InvalidTsvException("TSVs cannot contain duplicate primary key values");
            }
            return r;
        });
    }

}
