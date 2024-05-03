package org.databiosphere.workspacedataservice.service;

import static java.util.stream.Collectors.toSet;
import static org.databiosphere.workspacedataservice.service.RelationUtils.getTypeValueForArray;
import static org.databiosphere.workspacedataservice.service.RelationUtils.getTypeValueForList;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.ARRAY_OF_DATE;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.ARRAY_OF_DATE_TIME;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.ARRAY_OF_JSON;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.ARRAY_OF_RELATION;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.ARRAY_OF_STRING;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.BOOLEAN;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.DATE;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.DATE_TIME;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.EMPTY_ARRAY;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.FILE;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.JSON;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.NULL;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.NUMBER;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.RELATION;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.STRING;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.mu.util.stream.BiStream;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.attributes.JsonAttribute;
import org.springframework.util.CollectionUtils;

public class DataTypeInferer {

  private final ObjectMapper objectMapper;

  public DataTypeInferer(ObjectMapper mapper) {
    this.objectMapper = mapper;
  }

  public Map<String, DataTypeMapping> inferTypes(RecordAttributes updatedAtts) {
    Map<String, DataTypeMapping> result = new HashMap<>();
    for (Map.Entry<String, Object> entry : updatedAtts.attributeSet()) {
      result.put(entry.getKey(), inferType(entry.getValue()));
    }
    return result;
  }

  public Map<String, DataTypeMapping> inferTypes(List<Record> records) {
    Map<String, DataTypeMapping> result = new HashMap<>();
    for (Record rcd : records) {
      if (rcd.getAttributes() == null) {
        continue;
      }
      Map<String, DataTypeMapping> inferred = inferTypes(rcd.getAttributes());
      for (Map.Entry<String, DataTypeMapping> entry : inferred.entrySet()) {
        DataTypeMapping inferredType = entry.getValue();
        if (result.containsKey(entry.getKey()) && result.get(entry.getKey()) != inferredType) {
          result.put(entry.getKey(), selectBestType(result.get(entry.getKey()), inferredType));
        } else {
          result.putIfAbsent(entry.getKey(), inferredType);
        }
      }
    }
    return result;
  }

  public DataTypeMapping selectBestType(DataTypeMapping existing, DataTypeMapping newMapping) {
    if (existing == newMapping) {
      return existing;
    }
    // if we're comparing to a NULL type, favor the non-null if present
    if (newMapping == NULL || existing == NULL) {
      return newMapping != NULL ? newMapping : existing;
    }
    if (existing.isArrayType()
        && newMapping.isArrayType()
        && Set.of(existing, newMapping).contains(EMPTY_ARRAY)) {
      return newMapping != EMPTY_ARRAY ? newMapping : existing;
    }
    if (newMapping == DATE_TIME && existing == DATE) {
      return DATE_TIME;
    }
    if (newMapping == ARRAY_OF_DATE_TIME && existing == ARRAY_OF_DATE) {
      return ARRAY_OF_DATE_TIME;
    }
    if (newMapping.isArrayType() && existing.isArrayType()) {
      return ARRAY_OF_STRING;
    }
    return STRING;
  }

  // libreoffice at least uses left and right quotes which cause problems when we try to parse as
  // JSON
  public String replaceLeftRightQuotes(String val) {
    return val.replaceAll("[“”]", "\"");
  }

  /* This is secondary detection. The JSON and TSV deserializers have created String objects, but those
  Strings may represent dates, datetimes, etc. So, we inspect those Strings here.
  */
  // TODO: create an explicit deserialization step that creates dates, datetimes, etc. and simplify
  // here.
  private DataTypeMapping getTypeMappingFromString(String sVal) {
    if (isValidDate(sVal)) {
      return DATE;
    }
    if (isValidDateTime(sVal)) {
      return DATE_TIME;
    }
    if (isValidBoolean(sVal)) {
      return BOOLEAN;
    }
    if (tryJsonObject(sVal).isPresent()) {
      return JSON;
    }
    if (isFileType(sVal)) {
      return FILE;
    }
    return STRING;
  }

  /**
   * JSON input format has more type information so we do a little less guessing here than we do
   * with a TSV input
   *
   * <p>Order matters; we want to choose the most specific type. "1234" is valid json, but the code
   * chooses to infer it as a LONG (bigint in the db). "true" is a string and valid json but the
   * code is ordered to infer boolean. true is also valid json but we want to infer boolean.
   *
   * @param val the value for which to infer a type
   * @return the data type we want to use for this value
   */
  @VisibleForTesting
  DataTypeMapping inferType(Object val) {
    // null does not tell us much, this results in a text data type in the db if
    // everything in batch is null
    // if there are non-null values in the batch this return value will let those
    // values determine the
    // underlying SQL data type
    if (val == null) {
      return NULL;
    }

    if (val instanceof BigDecimal || val instanceof BigInteger) {
      return NUMBER;
    }

    if (val instanceof Boolean) {
      return BOOLEAN;
    }

    if (RelationUtils.isRelationValue(val)) {
      return RELATION;
    }

    if (val instanceof List<?> listVal) {
      return findArrayType(listVal);
    }

    if (val instanceof Map || val instanceof JsonAttribute) {
      return JSON;
    }

    return getTypeMappingFromString(val.toString());
  }

  public boolean isValidBoolean(String sVal) {
    return sVal.equalsIgnoreCase("true") || sVal.equalsIgnoreCase("false");
  }

  public boolean isNumericValue(String sVal) {
    try {
      new BigDecimal(sVal);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  @Nullable
  private JsonNode parseToJsonNode(String val) {
    try {
      // We call .toLowerCase() to ensure that WDS interprets all different inputted spellings of
      // boolean values
      // as booleans - e.g. `TRUE`, `tRUe`, or `true` ---> `true`
      return objectMapper.readTree(val.toLowerCase());
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  /**
   * Attempts to parse the input as json. If the input parsed as a valid json object (not array!),
   * return an Optional containing that ObjectNode; else return an empty Optional.
   *
   * @param val the input to be parsed
   * @return Optional of the ObjectNode, or empty if the input was not a json object
   */
  public Optional<ObjectNode> tryJsonObject(String val) {
    JsonNode jsonNode = parseToJsonNode(val);
    if (jsonNode instanceof ObjectNode objectNode) {
      return Optional.of(objectNode);
    }
    return Optional.empty();
  }

  public boolean isArray(String val) {
    JsonNode jsonNode = parseToJsonNode(val);
    return jsonNode != null && jsonNode.isArray();
  }

  private <T> DataTypeMapping findArrayType(List<T> list) {
    if (CollectionUtils.isEmpty(list)) {
      return EMPTY_ARRAY;
    }
    if (isArrayOfJson(list)) {
      return ARRAY_OF_JSON;
    }
    List<DataTypeMapping> inferredTypes = list.stream().map(this::inferType).distinct().toList();
    DataTypeMapping bestMapping = inferredTypes.get(0);
    if (inferredTypes.size() > 1) {
      for (DataTypeMapping type : inferredTypes) {
        bestMapping = selectBestType(bestMapping, type);
      }
    }
    return DataTypeMapping.getArrayTypeForBase(bestMapping);
  }

  /**
   * Should this List be treated by WDS as ARRAY_OF_JSON?
   *
   * @param list the input list
   * @return whether WDS should consider this an ARRAY_OF_JSON
   */
  private boolean isArrayOfJson(List<?> list) {
    // AJ-1748: here, we could also detect mixed arrays such as [1,"two",false] and treat those
    // as json instead of stringify-ing them. We can accomplish this by checking the distinct
    // classes of the list elements:
    //     list.stream().map(Object::getClass).distinct().toList();
    // and seeing if they are homogenous. If you do this, be careful of multiple
    // classes which we treat the same, e.g. BigInteger and BigDecimal.

    // is any element of this List itself a List or a Map? This indicates nested
    // structures, so we should treat this as an array of json.
    return list.stream()
        .anyMatch(element -> element instanceof List<?> || element instanceof Map<?, ?>);
  }

  public <T> T[] getArrayOfType(String val, Class<T[]> clazz) {
    try {
      String escapedValue = replaceLeftRightQuotes(val);
      if (clazz.getComponentType() == Boolean.class) {
        // Ensure that potential additional quotes do not surround the boolean values
        escapedValue = escapedValue.toLowerCase().replaceAll("\"", "");
      }
      return objectMapper.readValue(escapedValue, clazz);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  public boolean isValidDateTime(String val) {
    try {
      LocalDate.parse(val, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    } catch (DateTimeParseException e) {
      return false;
    }
    return true;
  }

  public boolean isValidDate(String val) {
    try {
      LocalDate.parse(val, DateTimeFormatter.ISO_LOCAL_DATE);
    } catch (DateTimeParseException e) {
      return false;
    }
    return true;
  }

  /**
   * Finds all attributes that reference another table
   *
   * @param records - all records whose references to check
   * @return Set of Relation for all referencing attributes
   */
  public RelationCollection findRelations(
      List<Record> records, Map<String, DataTypeMapping> schema) {
    List<String> relationAttributes =
        BiStream.from(schema).filterValues(mapping -> mapping == RELATION).keys().toList();
    List<String> relationArrayAttributes =
        BiStream.from(schema).filterValues(mapping -> mapping == ARRAY_OF_RELATION).keys().toList();

    Set<Relation> relations = new HashSet<>();
    Set<Relation> relationArrays = new HashSet<>();
    if (relationAttributes.isEmpty() && relationArrayAttributes.isEmpty()) {
      return new RelationCollection(relations, relationArrays);
    }
    for (Record rec : records) {
      // find all scalar attributes for this record whose names are in relationAttributes
      // and convert them to Relations, then save to the "relations" Set
      Set<Relation> relationsForThisRecord =
          BiStream.from(rec.attributeSet())
              .filterKeys(relationAttributes::contains)
              .filterValues(Objects::nonNull)
              .mapValues(RelationUtils::getTypeValue)
              .mapToObj(Relation::new)
              .collect(toSet());
      relations.addAll(relationsForThisRecord);

      // find all array attributes for this record whose names are in relationArrayAttributes
      // and convert them to Relations, then save to the "relationArrays" Set
      Set<Relation> relationArraysForThisRecord =
          BiStream.from(rec.attributeSet())
              .filterKeys(relationArrayAttributes::contains)
              .filterValues(Objects::nonNull)
              .mapValues(this::getMultiValueType)
              .mapToObj(Relation::new)
              .collect(toSet());
      relationArrays.addAll(relationArraysForThisRecord);
    }
    return new RelationCollection(relations, relationArrays);
  }

  private RecordType getMultiValueType(Object value) {
    if (value instanceof List<?> listVal) { // from a json source
      return getTypeValueForList(listVal);
    } else { // from a tsv source
      return getTypeValueForArray(getArrayOfType(value.toString(), String[].class));
    }
  }

  private boolean isFileType(String possibleFile) {
    URI fileUri;
    try {
      fileUri = new URI(possibleFile);
      // Many non-URI strings will parse without exception but have no scheme or host
      if (fileUri.getScheme() == null || fileUri.getHost() == null) {
        return false;
      }
    } catch (URISyntaxException use) {
      return false;
    }
    // https://[].blob.core.windows.net/[] or drs://[]
    return fileUri.getScheme().equalsIgnoreCase("drs")
        || (fileUri.getScheme().equalsIgnoreCase("https")
            && fileUri.getHost().toLowerCase().endsWith(".blob.core.windows.net"));
  }
}
