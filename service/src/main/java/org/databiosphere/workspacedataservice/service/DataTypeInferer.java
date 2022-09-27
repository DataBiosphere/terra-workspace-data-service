package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import org.databiosphere.workspacedataservice.shared.model.Record;
import java.util.Map;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;

public class DataTypeInferer {

	private ObjectMapper objectMapper = new ObjectMapper();

	public Map<String, DataTypeMapping> inferTypes(Map<String, Object> updatedAtts) {
		Map<String, DataTypeMapping> result = new HashMap<>();
		for (Map.Entry<String, Object> entry : updatedAtts.entrySet()) {
			result.put(entry.getKey(), inferType(entry.getValue()));
		}
		return result;
	}

	public Map<String, DataTypeMapping> inferTypes(List<Record> records){
		Map<String, DataTypeMapping> result = new HashMap<>();
		for (Record record : records) {
			if(record.getAttributes() == null){
				continue;
			}
			for (Map.Entry<String, Object> entry : record.getAttributes().getAttributes().entrySet()) {
				DataTypeMapping inferredType = inferType(entry.getValue());
				if(result.containsKey(entry.getKey()) && result.get(entry.getKey()) != inferredType){
					result.put(entry.getKey(), selectBestType(result.get(entry.getKey()), inferredType));
				} else {
					result.put(entry.getKey(), inferredType);
				}
			}
		}
		return result;
	}

	public DataTypeMapping selectBestType(DataTypeMapping existing, DataTypeMapping newMapping) {
		if (existing == newMapping) {
			return existing;
		}
		if (existing == LONG && newMapping == DOUBLE) {
			return DOUBLE;
		}
		if (existing == DOUBLE && newMapping == LONG) {
			return DOUBLE;
		}
		return STRING;
	}

	/**
	 * Order matters; we want to choose the most specific type. "1234" is valid
	 * json, but the code chooses to infer it as a LONG (bigint in the db). "true"
	 * is a string and valid json but the code is ordered to infer boolean. true is
	 * also valid json but we want to infer boolean.
	 *
	 * @param val
	 * @return
	 */
	public DataTypeMapping inferType(Object val) {
		// if we're looking at a user request and they submit a null value for a new
		// attribute,
		// this is the best inference we can make about the column type
		if (val == null) {
			return STRING;
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

		String sVal = val.toString();
		if (isValidDate(sVal)) {
			return DATE;
		}
		if (isValidDateTime(sVal)) {
			return DATE_TIME;
		}
		if (sVal.equalsIgnoreCase("true") || sVal.equalsIgnoreCase("false")) {
			return BOOLEAN;
		}
		if (isValidJson(sVal)) {
			return JSON;
		}
		return STRING;
	}

	public boolean isValidJson(String val) {
		try {
			JsonNode jsonNode = objectMapper.readTree(val);
			return jsonNode.isArray() || jsonNode.isObject();
		} catch (JsonProcessingException e) {
			return false;
		}
	}

	private boolean isValidDateTime(String val) {
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
