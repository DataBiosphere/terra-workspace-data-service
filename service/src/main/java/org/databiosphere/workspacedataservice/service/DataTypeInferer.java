package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ArrayUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.*;

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
		if (existing.isArrayType() && newMapping.isArrayType() && Set.of(existing, newMapping).contains(EMPTY_ARRAY)) {
			return newMapping != EMPTY_ARRAY ? newMapping : existing;
		}
		if(newMapping == DATE_TIME && existing == DATE){
			return DATE_TIME;
		}
		if(newMapping == ARRAY_OF_DATE_TIME && existing == ARRAY_OF_DATE){
			return ARRAY_OF_DATE_TIME;
		}
		if(newMapping.isArrayType() && existing.isArrayType()) {
			return ARRAY_OF_STRING;
		}
		return STRING;
	}

	//libreoffice at least uses left and right quotes which cause problems when we try to parse as JSON
	public String replaceLeftRightQuotes(String val){
		return val.replaceAll("[“”]", "\"");
	}

	/* This is secondary detection. The JSON and TSV deserializers have created String objects, but those
		Strings may represent dates, datetimes, etc. So, we inspect those Strings here.
	 */
	// TODO: create an explicit deserialization step that creates dates, datetimes, etc. and simplify here.
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
	public DataTypeMapping inferType(Object val) {
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

		if(val instanceof List<?> listVal){
			return findArrayType(listVal);
		}

		if(val instanceof Map){
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

	private JsonNode parseToJsonNode(String val){
		try {
			// We call .toLowerCase() to ensure that WDS interprets all different inputted spellings of boolean values
			// as booleans - e.g. `TRUE`, `tRUe`, or `true` ---> `true`
			return objectMapper.readTree(val.toLowerCase());
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

	private <T> DataTypeMapping findArrayType(List<T> list){
		if(CollectionUtils.isEmpty(list)){
			return EMPTY_ARRAY;
		}
		List<DataTypeMapping> inferredTypes = list.stream()
				.map(item -> inferType(item))
				.distinct()
				.toList();
		DataTypeMapping bestMapping = inferredTypes.get(0);
		if (inferredTypes.size() > 1) {
			for (DataTypeMapping type : inferredTypes) {
				bestMapping = selectBestType(bestMapping, type);
			}
		}
		return DataTypeMapping.getArrayTypeForBase(bestMapping);
	}

	public <T> T[] getArrayOfType(String val, Class<T[]> clazz) {
		try {
			String escapedValue = replaceLeftRightQuotes(val);
			if(clazz.getComponentType() == Boolean.class){
				// Ensure that potential additional quotes do not surround the boolean values
				escapedValue = escapedValue.toLowerCase().replaceAll("\"","");
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
	 * @param records
	 *            - all records whose references to check
	 * @return Set of Relation for all referencing attributes
	 */
	public RelationCollection findRelations(List<Record> records, Map<String, DataTypeMapping> schema) {
		List<String> relationAttributes = schema.entrySet().stream().filter(attr -> attr.getValue() == RELATION).map(Map.Entry::getKey).toList();
		List<String> relationArrayAttributes = schema.entrySet().stream().filter(attr -> attr.getValue() == ARRAY_OF_RELATION).map(Map.Entry::getKey).toList();
		Set<Relation> relations = new HashSet<>();
		Set<Relation> relationArrays = new HashSet<>();
		if (relationAttributes.isEmpty() && relationArrayAttributes.isEmpty()) {
			return new RelationCollection(relations, relationArrays);
		}
		for (Record rec : records) {
			// find all scalar attributes for this record whose names are in relationAttributes
			// and convert them to Relations, then save to the "relations" Set
			Set<Relation> relationsForThisRecord = rec.attributeSet().stream()
					.filter( entry -> relationAttributes.contains(entry.getKey()))
					.map(entry -> new Relation(entry.getKey(), RelationUtils.getTypeValue(entry.getValue())))
					.collect(Collectors.toSet());
			relations.addAll(relationsForThisRecord);

			// find all array attributes for this record whose names are in relationArrayAttributes
			// and convert them to Relations, then save to the "relationArrays" Set
			Set<Relation> relationArraysForThisRecord = rec.attributeSet().stream()
					.filter( entry -> relationArrayAttributes.contains(entry.getKey()))
					.map(entry -> {
						if (entry.getValue() instanceof List<?> listVal) { //from a json source,
							return new Relation(entry.getKey(), RelationUtils.getTypeValueForList(listVal));
						} else { //from a tsv source
							return new Relation(entry.getKey(), RelationUtils.getTypeValueForArray(getArrayOfType(entry.getValue().toString(), String[].class)));
						}
					})
					.collect(Collectors.toSet());
			relationArrays.addAll(relationArraysForThisRecord);
		}
		return new RelationCollection(relations, relationArrays);
	}
}
