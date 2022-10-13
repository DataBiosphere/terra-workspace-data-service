package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.*;

public class DataTypeInferer {

	private ObjectMapper objectMapper;

	public DataTypeInferer() {
		objectMapper = JsonMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
				.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
				.configure(DeserializationFeature.ACCEPT_FLOAT_AS_INT, false)
				.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false).findAndAddModules().build();
	}

	public Map<String, DataTypeMapping> inferTypes(RecordAttributes updatedAtts, InBoundDataSource dataSource) {
		Map<String, DataTypeMapping> result = new HashMap<>();
		for (Map.Entry<String, Object> entry : updatedAtts.attributeSet()) {
			result.put(entry.getKey(), inferType(entry.getValue(), dataSource));
		}
		return result;
	}

	public Map<String, DataTypeMapping> inferTypes(List<Record> records, InBoundDataSource dataSource) {
		Map<String, DataTypeMapping> result = new HashMap<>();
		for (Record rcd : records) {
			if (rcd.getAttributes() == null) {
				continue;
			}
			Map<String, DataTypeMapping> inferred = inferTypes(rcd.getAttributes(), dataSource);
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
		if (Set.of(newMapping, existing).equals(Set.of(LONG, DOUBLE))) {
			return DOUBLE;
		}
		return STRING;
	}

	/**
	 * Our TSV parser gives everything to us as Strings so we try to guess at the
	 * types from the String representation and choose the stronger typing when we
	 * can. "" is converted to null which also differs from our JSON handling.
	 * 
	 * @se inferTypeForJsonSource
	 * @param val
	 * @return
	 */
	public DataTypeMapping inferTypeForTsvSource(Object val) {
		// For TSV we treat null and "" as the null value
		if (StringUtils.isEmpty((String) val)) {
			return NULL;
		}
		String sVal = val.toString();

		if (RelationUtils.isRelationValue(val)) {
			return STRING;
		}
		// when we load from TSV, numbers are converted to strings, we need to go back
		// to numbers
		if (isLongValue(sVal)) {
			return LONG;
		}
		if (isDoubleValue(sVal)) {
			return DOUBLE;
		}
		return getTypeMappingFromString(sVal);
	}

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
		if (isValidJson(sVal)) {
			return JSON;
		}
		if(isArray(sVal)){
			return findArrayType(sVal);
		}
		return STRING;
	}

	/**
	 * JSON input format has more type information so we do a little less guessing
	 * here than we do with a TSV input
	 *
	 * Order matters; we want to choose the most specific type. "1234" is valid
	 * json, but the code chooses to infer it as a LONG (bigint in the db). "true"
	 * is a string and valid json but the code is ordered to infer boolean. true is
	 * also valid json but we want to infer boolean.
	 *
	 * @param val
	 * @return the data type we want to use for this value
	 */
	public DataTypeMapping inferTypeForJsonSource(Object val) {
		// null does not tell us much, this results in a text data type in the db if
		// everything in batch is null
		// if there are non-null values in the batch this return value will let those
		// values determine the
		// underlying SQL data type
		if (val == null) {
			return NULL;
		}

		if (val instanceof Long || val instanceof Integer) {
			return LONG;
		}

		if (val instanceof Double || val instanceof Float) {
			return DOUBLE;
		}

		if (val instanceof Boolean) {
			return BOOLEAN;
		}

		if (RelationUtils.isRelationValue(val)) {
			return STRING;
		}

		return getTypeMappingFromString(val.toString());
	}

	public boolean isValidBoolean(String sVal) {
		return sVal.equalsIgnoreCase("true") || sVal.equalsIgnoreCase("false");
	}

	public DataTypeMapping inferType(Object val, InBoundDataSource dataSource) {
		if (dataSource == InBoundDataSource.TSV) {
			return inferTypeForTsvSource(val);
		} else if (dataSource == InBoundDataSource.JSON) {
			return inferTypeForJsonSource(val);
		}
		throw new IllegalArgumentException("Unhandled inbound data source " + dataSource);
	}

	public boolean isLongValue(String sVal) {
		try {
			Long.parseLong(sVal);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	public boolean isDoubleValue(String sVal) {
		try {
			Double.parseDouble(sVal);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	private JsonNode parseToJsonNode(String val){
		try {
			return objectMapper.readTree(val);
		} catch (JsonProcessingException e) {
			return null;
		}
	}

	public boolean isValidJson(String val) {
		JsonNode jsonNode = parseToJsonNode(val);
		return jsonNode != null && jsonNode.isObject();
	}

	public boolean isArray(String val){
		JsonNode jsonNode = parseToJsonNode(val);
		return jsonNode != null && jsonNode.isArray();
	}

	private DataTypeMapping findArrayType(String val)  {
		if(getArrayOfType(val, Boolean[].class) != null){
			return ARRAY_OF_BOOLEAN;
		}
		if(getArrayOfType(val, Long[].class) != null){
			return ARRAY_OF_LONG;
		}
		if(getArrayOfType(val, Double[].class) != null){
			return ARRAY_OF_DOUBLE;
		}
		if(getArrayOfType(val, LocalDateTime[].class) != null){
			return ARRAY_OF_DATE_TIME;
		}
		if(getArrayOfType(val, LocalDate[].class) != null){
			return ARRAY_OF_DATE;
		}
		if(getArrayOfType(val, String[].class) != null){
			return ARRAY_OF_TEXT;
		}
		throw new IllegalArgumentException("Unsupported array type");
	}

	public <T> T[] getArrayOfType(String val, Class<T[]> clazz) {
		try {
			return objectMapper.readValue(val, clazz);
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
}
