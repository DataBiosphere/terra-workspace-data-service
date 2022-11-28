package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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

import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.*;

public class DataTypeInferer {

	private final ObjectMapper objectMapper;

	public DataTypeInferer(ObjectMapper mapper) {
		this.objectMapper = mapper;
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
			return RELATION;
		}
		// when we load from TSV, numbers are converted to strings, we need to go back
		// to numbers
		if (isNumericValue(sVal)) {
			return NUMBER;
		}
		return getTypeMappingFromString(replaceLeftRightQuotes(sVal));
	}

	//libreoffice at least uses left and right quotes which cause problems when we try to parse as JSON
	private String replaceLeftRightQuotes(String val){
		return val.replaceAll("[“”]", "\"");
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
			return findArrayTypeFromJson(sVal);
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

	public DataTypeMapping inferType(Object val, InBoundDataSource dataSource) {
		if (dataSource == InBoundDataSource.TSV) {
			return inferTypeForTsvSource(val);
		} else if (dataSource == InBoundDataSource.JSON) {
			return inferTypeForJsonSource(val);
		}
		throw new IllegalArgumentException("Unhandled inbound data source " + dataSource);
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

	private <T> DataTypeMapping findArrayType(List<T> list){
		if(CollectionUtils.isEmpty(list)){
			return EMPTY_ARRAY;
		}
		List<DataTypeMapping> inferredTypes = list.stream()
				.map(item -> inferType(item, InBoundDataSource.JSON))
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

	private DataTypeMapping findArrayTypeFromJson(String val)  {
		if(ArrayUtils.isNotEmpty(getArrayOfType(val, Boolean[].class))){
			return ARRAY_OF_BOOLEAN;
		}
		if(ArrayUtils.isNotEmpty(getArrayOfType(val, Double[].class))){
			return ARRAY_OF_NUMBER;
		}
		//order matters an array of LocalDateTime will be parsed to LocalDate
		if(ArrayUtils.isNotEmpty(getArrayOfType(val, LocalDateTime[].class))){
			return ARRAY_OF_DATE_TIME;
		}
		if(ArrayUtils.isNotEmpty(getArrayOfType(val, LocalDate[].class))){
			return ARRAY_OF_DATE;
		}
		String[] stringArr = getArrayOfType(val, String[].class);
		if(ArrayUtils.isNotEmpty(stringArr)){
			if (Arrays.stream(stringArr).allMatch(RelationUtils::isRelationValue)){
				return ARRAY_OF_RELATION;
			}
			return ARRAY_OF_STRING;
		}
		if(stringArr != null){
			return EMPTY_ARRAY;
		}
		throw new IllegalArgumentException("Unsupported array type " + val);
	}

	public <T> T[] getArrayOfType(String val, Class<T[]> clazz) {
		try {
			return objectMapper.readValue(replaceLeftRightQuotes(val), clazz);
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
		Set<Relation> relations = new HashSet<>();
		Set<Relation> relationArrays = new HashSet<>();
		for (Record record : records) {
			for (Map.Entry<String, Object> entry : record.attributeSet()) {
				if (schema.get(entry.getKey()) == RELATION){
					relations.add(new Relation(entry.getKey(), RelationUtils.getTypeValue(entry.getValue())));
					//TODO deal with tsv vs json source a bit smarter
				} else if (schema.get(entry.getKey()) == ARRAY_OF_RELATION){
					if (entry.getValue() instanceof List<?> listVal) { //from a json source,
					    relationArrays.add(new Relation(entry.getKey(), RelationUtils.getTypeValueForList(listVal)));
					} else { //from a tsv source
						relationArrays.add(new Relation(entry.getKey(), RelationUtils.getTypeValueForArray(getArrayOfType(entry.getValue().toString(), String[].class))));
					}
				}
			}
		}
		return new RelationCollection(relations, relationArrays);
	}
}
