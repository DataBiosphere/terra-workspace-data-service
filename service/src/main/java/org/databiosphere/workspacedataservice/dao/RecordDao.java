package org.databiosphere.workspacedataservice.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.service.model.RelationValue;
import org.databiosphere.workspacedataservice.service.model.exception.BatchDeleteException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.AttributeComparator;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordColumn;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.postgresql.jdbc.PgArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.PRIMARY_KEY_COLUMN_CACHE;
import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;
import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException.NameType.ATTRIBUTE;
import static org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException.NameType.RECORD_TYPE;

@Repository
public class RecordDao {

	private static final String INSTANCE_ID = "instanceId";
	private static final String RECORD_ID_PARAM = "recordId";
	private final NamedParameterJdbcTemplate namedTemplate;

	private final NamedParameterJdbcTemplate templateForStreaming;

	private static final Logger LOGGER = LoggerFactory.getLogger(RecordDao.class);

	private final DataTypeInferer inferer;

	private final ObjectMapper objectMapper;
	private final CachedQueryDao cachedQueryDao;

	public RecordDao(NamedParameterJdbcTemplate namedTemplate,
			@Qualifier("streamingDs") NamedParameterJdbcTemplate templateForStreaming, DataTypeInferer inf, ObjectMapper objectMapper, CachedQueryDao cachedQueryDao) {
		this.namedTemplate = namedTemplate;
		this.templateForStreaming = templateForStreaming;
		this.inferer = inf;
		this.objectMapper = objectMapper;
		this.cachedQueryDao = cachedQueryDao;
	}

	public boolean instanceSchemaExists(UUID instanceId) {
		return Boolean.TRUE.equals(namedTemplate.queryForObject(
				"select exists(select from information_schema.schemata WHERE schema_name = :workspaceSchema)",
				new MapSqlParameterSource("workspaceSchema", instanceId.toString()), Boolean.class));
	}

	public void createSchema(UUID instanceId) {
		namedTemplate.getJdbcTemplate().update("create schema " + quote(instanceId.toString()));
	}


	@SuppressWarnings("squid:S2077") // since instanceId must be a UUID, it is safe to use inline
	public void dropSchema(UUID instanceId) {
		namedTemplate.getJdbcTemplate().update("drop schema " + quote(instanceId.toString()) + " cascade");
	}

	public boolean recordTypeExists(UUID instanceId, RecordType recordType) {
		return Boolean.TRUE.equals(namedTemplate.queryForObject(
				"select exists(select from pg_tables where schemaname = :instanceId AND tablename  = :recordType)",
				new MapSqlParameterSource(
						Map.of(INSTANCE_ID, instanceId.toString(), "recordType", recordType.getName())),
				Boolean.class));
	}

	@SuppressWarnings("squid:S2077")
	@Transactional
	public void createRecordType(UUID instanceId, Map<String, DataTypeMapping> tableInfo, RecordType recordType,
			RelationCollection relations, String recordTypePrimaryKey) {
		//this handles the case where the user incorrectly includes the primary key data in the attributes
		tableInfo = Maps.filterKeys(tableInfo, k -> !k.equals(recordTypePrimaryKey));
		String columnDefs = genColumnDefs(tableInfo, recordTypePrimaryKey);
		try {
			namedTemplate.getJdbcTemplate().update("create table " + getQualifiedTableName(recordType, instanceId)
					+ "( " + columnDefs + (!relations.relations().isEmpty() ? ", " + getFkSql(relations.relations(), instanceId) : "") + ")");
			for (Relation relationArray : relations.relationArrays()){
				createRelationJoinTable(instanceId, relationArray.relationColName(), recordType, relationArray.relationRecordType());
			}
		} catch (DataAccessException e) {
			if (e.getRootCause()instanceof SQLException sqlEx) {
				checkForMissingTable(sqlEx);
			}
			//this exception is thrown from getFkSql if the referenced relation doesn't exist
			if(e instanceof EmptyResultDataAccessException){
				throw new MissingObjectException("Record type for relation");
			}
			throw e;
		}
	}

	private String getJoinTableName(String relationColumnName, RecordType fromTable){
		//Use RESERVED_NAME_PREFIX to ensure no collision with user-named tables.
		//RecordType name has already been sql-validated
		return quote(RESERVED_NAME_PREFIX + fromTable.getName() + "_" + SqlUtils.validateSqlString(relationColumnName, ATTRIBUTE));
	}

	private String getQualifiedJoinTableName(UUID instanceId, String relationColumnName, RecordType fromTable){
		return quote(instanceId.toString()) + "." + getJoinTableName(relationColumnName, fromTable);
	}

	private String getFromColumnName(RecordType referringRecordType){
		return "from_" + referringRecordType.getName() + "_key";
	}

	private String getToColumnName(RecordType referencedRecordType){
		return "to_" + referencedRecordType.getName() + "_key";

	}

	@SuppressWarnings("squid:S2077")
	public void createRelationJoinTable(UUID instanceId, String tableName, RecordType referringRecordType,
								 RecordType referencedRecordType) {
		String fromCol = getFromColumnName(referringRecordType);
		String toCol = getToColumnName(referencedRecordType);
		String columnDefs =  quote(fromCol) + " text, " + quote(toCol) + " text";
		//Possibly temporary fake relations to make existing methods work for this situation
		Set<Relation> relations = Set.of(new Relation(fromCol, referringRecordType), new Relation(toCol, referencedRecordType));
		try {
			namedTemplate.getJdbcTemplate().update("create table " + getQualifiedJoinTableName(instanceId, tableName, referringRecordType) +
					"( " + columnDefs + ", " + getFkSql(relations, instanceId) + ")");
		} catch (DataAccessException e) {
			if (e.getRootCause()instanceof SQLException sqlEx) {
				checkForMissingTable(sqlEx);
			}
			//this exception is thrown from getFkSql if the referenced relation doesn't exist
			if(e instanceof EmptyResultDataAccessException){
				throw new MissingObjectException("Record type for relation");
			}
			throw e;
		}
	}

	private String getQualifiedTableName(RecordType recordType, UUID instanceId) {
		// N.B. recordType is sql-validated in its constructor, so we don't need it here
		return quote(instanceId.toString()) + "."
				+ quote(SqlUtils.validateSqlString(recordType.getName(), RECORD_TYPE));
	}

	@SuppressWarnings("squid:S2077")
	public List<Record> queryForRecords(RecordType recordType, int pageSize, int offset, String sortDirection,
			String sortAttribute, UUID instanceId) {
		LOGGER.info("queryForRecords: {}", recordType.getName());
		return namedTemplate.getJdbcTemplate().query(
				"select * from " + getQualifiedTableName(recordType, instanceId) + " order by "
						+ (sortAttribute == null ? quote(cachedQueryDao.getPrimaryKeyColumn(recordType, instanceId)) : quote(sortAttribute)) + " " + sortDirection + " limit "
						+ pageSize + " offset " + offset,
				new RecordRowMapper(recordType, objectMapper, instanceId));
	}

	public List<String> getAllAttributeNames(UUID instanceId, RecordType recordType) {
		MapSqlParameterSource params = new MapSqlParameterSource(INSTANCE_ID, instanceId.toString());
		params.addValue("tableName", recordType.getName());
		List<String> attributeNames = namedTemplate.queryForList("select column_name from INFORMATION_SCHEMA.COLUMNS where table_schema = :instanceId "
				+ "and table_name = :tableName", params, String.class);
		Collections.sort(attributeNames, new AttributeComparator(cachedQueryDao.getPrimaryKeyColumn(recordType, instanceId)));
		return attributeNames;
	}

	public Map<String, DataTypeMapping> getExistingTableSchema(UUID instanceId, RecordType recordType) {
		MapSqlParameterSource params = new MapSqlParameterSource(INSTANCE_ID, instanceId.toString());
		params.addValue("tableName", recordType.getName());
		String sql = "select column_name, udt_name::regtype as data_type from INFORMATION_SCHEMA.COLUMNS " +
				"where table_schema = :instanceId and table_name = :tableName";
		return getTableSchema(sql, params);
	}

	private Map<String, DataTypeMapping> getTableSchema(String sql, MapSqlParameterSource params){
		return namedTemplate.query(sql, params, rs -> {
			Map<String, DataTypeMapping> result = new HashMap<>();
			while (rs.next()) {
				result.put(rs.getString("column_name"),
						DataTypeMapping.fromPostgresType(rs.getString("data_type")));
			}
			return result;
		});
	}

	public Map<String, DataTypeMapping> getExistingTableSchemaLessPrimaryKey(UUID instanceId, RecordType recordType) {
		MapSqlParameterSource params = new MapSqlParameterSource(INSTANCE_ID, instanceId.toString());
		params.addValue("tableName", recordType.getName());
		params.addValue("primaryKey", cachedQueryDao.getPrimaryKeyColumn(recordType, instanceId));
		String sql = "select column_name, coalesce(domain_name, udt_name::regtype::varchar) as data_type from INFORMATION_SCHEMA.COLUMNS where table_schema = :instanceId "
				+ "and table_name = :tableName and column_name != :primaryKey";
		return getTableSchema(sql, params);
	}

	public void addColumn(UUID instanceId, RecordType recordType, String columnName, DataTypeMapping colType) {
		addColumn(instanceId, recordType, columnName, colType, null);
	}

	@SuppressWarnings("squid:S2077")
	public void addColumn(UUID instanceId, RecordType recordType, String columnName, DataTypeMapping colType,
			RecordType referencedType) {
		try {
			namedTemplate.getJdbcTemplate()
					.update("alter table " + getQualifiedTableName(recordType, instanceId) + " add column "
							+ quote(SqlUtils.validateSqlString(columnName, ATTRIBUTE)) + " " + colType.getPostgresType()
							+ (referencedType != null
									? " references " + getQualifiedTableName(referencedType, instanceId)
									: ""));
		} catch (DataAccessException e) {
			if (e.getRootCause()instanceof SQLException sqlEx) {
				checkForMissingTable(sqlEx);
			}
			throw e;
		}
	}

	@SuppressWarnings("squid:S2077")
	public void changeColumn(UUID instanceId, RecordType recordType, String columnName, DataTypeMapping newColType) {
		namedTemplate.getJdbcTemplate()
				.update("alter table " + getQualifiedTableName(recordType, instanceId) + " alter column "
						+ quote(SqlUtils.validateSqlString(columnName, ATTRIBUTE)) + " TYPE "
						+ newColType.getPostgresType());
	}

	private String genColumnDefs(Map<String, DataTypeMapping> tableInfo, String primaryKeyCol) {
		return getPrimaryKeyDef(primaryKeyCol)
				+ (tableInfo.size() > 0
						? ", " + tableInfo.entrySet().stream()
								.map(e -> quote(SqlUtils.validateSqlString(e.getKey(), ATTRIBUTE)) + " "
										+ e.getValue().getPostgresType())
								.collect(Collectors.joining(", "))
						: "");
	}

	private String getPrimaryKeyDef(String primaryKeyCol){
		return (primaryKeyCol.equals(RECORD_ID) ? quote(primaryKeyCol) : quote(SqlUtils.validateSqlString(primaryKeyCol, ATTRIBUTE))) + " text primary key";
	}

	private String quote(String toQuote) {
		return "\"" + toQuote + "\"";
	}

	// The expectation is that the record type already matches the schema and
	// attributes given, as
	// that's dealt with earlier in the code.
	public void batchUpsert(UUID instanceId, RecordType recordType, List<Record> records,
			Map<String, DataTypeMapping> schema, String primaryKeyColumn) {
		List<RecordColumn> schemaAsList = getSchemaWithRowId(schema, primaryKeyColumn);
		try {
			namedTemplate.getJdbcTemplate().batchUpdate(genInsertStatement(instanceId, recordType, schemaAsList, primaryKeyColumn),
					getInsertBatchArgs(records, schemaAsList, primaryKeyColumn));
		} catch (DataAccessException e) {
			if (e.getRootCause()instanceof SQLException sqlEx) {
				checkForMissingRecord(sqlEx);
			}
			throw e;
		}
	}

	public void insertIntoJoin(UUID instanceId, Relation column, RecordType recordType, List<RelationValue> relations){
		try {
			namedTemplate.getJdbcTemplate().batchUpdate(genJoinInsertStatement(instanceId, column, recordType),
					getJoinInsertBatchArgs(relations));
		} catch (DataAccessException e) {
			if (e.getRootCause()instanceof SQLException sqlEx) {
				checkForMissingRecord(sqlEx);
			}
			throw e;
		}
	}

	public void batchUpsert(UUID instanceId, RecordType recordType, List<Record> records,
							Map<String, DataTypeMapping> schema){
		batchUpsert(instanceId, recordType, records, schema, cachedQueryDao.getPrimaryKeyColumn(recordType, instanceId));
	}


	private List<RecordColumn> getSchemaWithRowId(Map<String, DataTypeMapping> schema, String recordIdColumn) {
		//we collect to a set first to handle the case where the user has included their primary key data in attributes
		return Stream.concat(Stream.of(new RecordColumn(recordIdColumn, DataTypeMapping.STRING)),
				schema.entrySet().stream().map(e -> new RecordColumn(e.getKey(), e.getValue())))
				.collect(Collectors.toSet()).stream().toList();
	}

	public boolean deleteSingleRecord(UUID instanceId, RecordType recordType, String recordId) {
		String recordTypePrimaryKey = cachedQueryDao.getPrimaryKeyColumn(recordType, instanceId);
		List<Relation> relationArrayCols = getRelationArrayCols(instanceId, recordType);
		if (!relationArrayCols.isEmpty()){
			//Remove values from join table first
			for (Relation rel : relationArrayCols){
				namedTemplate.update("delete from " + getQualifiedJoinTableName(instanceId, rel.relationColName(), recordType) + " where "
						+ quote(getFromColumnName(recordType)) + " = :recordId", new MapSqlParameterSource(RECORD_ID_PARAM, recordId));
			}
		}
		try {
			return namedTemplate.update("delete from " + getQualifiedTableName(recordType, instanceId) + " where "
					+ quote(recordTypePrimaryKey) + " = :recordId", new MapSqlParameterSource(RECORD_ID_PARAM, recordId)) == 1;
		} catch (DataIntegrityViolationException e) {
			if (e.getRootCause()instanceof SQLException sqlEx) {
				checkForTableRelation(sqlEx);
			}
			throw e;
		}
	}

	public void addForeignKeyForReference(RecordType recordType, RecordType referencedRecordType, UUID instanceId,
			String relationColName) {
		try {
			String addFk = "alter table " + getQualifiedTableName(recordType, instanceId) + " add foreign key ("
					+ quote(relationColName) + ") " + "references "
					+ getQualifiedTableName(referencedRecordType, instanceId);
			namedTemplate.getJdbcTemplate().execute(addFk);
		} catch (DataAccessException e) {
			if (e.getRootCause()instanceof SQLException sqlEx) {
				checkForMissingTable(sqlEx);
			}
			throw e;
		}
	}

	private void checkForMissingTable(SQLException sqlEx) {
		if (sqlEx != null && sqlEx.getSQLState() != null && sqlEx.getSQLState().equals("42P01")) {
			throw new MissingObjectException("Record type for relation");
		}
	}

	private void checkForMissingRecord(SQLException sqlEx) {
		if (sqlEx != null && sqlEx.getSQLState() != null && sqlEx.getSQLState().equals("23503")) {
			throw new InvalidRelationException(
					"It looks like you're trying to reference a record that does not exist.");
		}
	}

	private void checkForTableRelation(SQLException sqlEx) {
		if (sqlEx != null && sqlEx.getSQLState() != null) {
			if (sqlEx.getSQLState().equals("23503")) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
						"Unable to delete this record because another record has a relation to it.");
			}
			if (sqlEx.getSQLState().equals("2BP01")) {
				throw new ResponseStatusException(HttpStatus.CONFLICT,
						"Unable to delete this record type because another record type has a relation to it.");
			}
		}
	}

	public Stream<Record> streamAllRecordsForType(UUID instanceId, RecordType recordType) {
		return templateForStreaming.getJdbcTemplate().queryForStream(
				"select * from " + getQualifiedTableName(recordType, instanceId) + " order by " + quote(cachedQueryDao.getPrimaryKeyColumn(recordType, instanceId)),
				new RecordRowMapper(recordType, objectMapper, instanceId));
	}

	public String getFkSql(Set<Relation> relations, UUID instanceId) {

		return relations.stream()
				.map(r -> "constraint " + quote("fk_" + SqlUtils.validateSqlString(r.relationColName(), ATTRIBUTE))
						+ " foreign key (" + quote(SqlUtils.validateSqlString(r.relationColName(), ATTRIBUTE))
						+ ") references " + getQualifiedTableName(r.relationRecordType(), instanceId) + "(" + quote(cachedQueryDao.getPrimaryKeyColumn(r.relationRecordType(), instanceId))
						+ ")")
				.collect(Collectors.joining(", \n"));
	}

	public List<Relation> getRelationCols(UUID instanceId, RecordType recordType) {
		return namedTemplate.query(
				"SELECT kcu.column_name, ccu.table_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu "
						+ "ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
						+ "JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema "
						+ "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = :workspace AND tc.table_name= :tableName",
				Map.of("workspace", instanceId.toString(), "tableName", recordType.getName()),
				(rs, rowNum) -> new Relation(rs.getString("column_name"),
						RecordType.valueOf(rs.getString("table_name"))));
	}

	public List<Relation> getRelationArrayCols(UUID instanceId, RecordType recordType) {
		return namedTemplate.query(	"select kcu1.table_name, kcu1.column_name from information_schema.key_column_usage kcu1 join information_schema.key_column_usage kcu2 "
				+ "on kcu1.table_name = kcu2.table_name where kcu1.constraint_schema = :workspace and kcu2.constraint_name = :from_table_constraint"
				+ " and kcu2.constraint_name != kcu1.constraint_name",
		Map.of("workspace", instanceId.toString(), "from_table_constraint",  "fk_from_" + recordType.getName() + "_key"),
				new RelationRowMapper(recordType));
	}

	private class RelationRowMapper implements RowMapper<Relation> {

		private RecordType recordType;
		public RelationRowMapper(RecordType recordType){
			this.recordType = recordType;
		}
		@Override
		public Relation mapRow(ResultSet rs, int rowNum) throws SQLException {
			return new Relation(getAttributeFromTableName(rs.getString("table_name")), getRecordTypeFromConstraint(rs.getString("column_name")));
		}

		private RecordType getRecordTypeFromConstraint(String constraint){
			//constraint should be to_tablename_key
			return RecordType.valueOf(StringUtils.removeEnd(StringUtils.removeStart(constraint, "to_"), "_key"));
		}

		private String getAttributeFromTableName(String tableName){
			//table will be RESERVED_NAME_PREFIX_fromTable_attribute
			return StringUtils.removeStart(tableName,RESERVED_NAME_PREFIX + this.recordType.getName() + "_");
		}


	}

	@SuppressWarnings("squid:S2077")
	public int countRecords(UUID instanceId, RecordType recordType) {
		return namedTemplate.getJdbcTemplate()
				.queryForObject("select count(*) from " + getQualifiedTableName(recordType, instanceId), Integer.class);
	}

	private String genColUpsertUpdates(List<String> cols, String recordTypeRowIdentifier) {
		return cols.stream().filter(c -> !recordTypeRowIdentifier.equals(c)).map(c -> quote(c) + " = excluded." + quote(c))
				.collect(Collectors.joining(", "));
	}

	private List<Object[]> getInsertBatchArgs(List<Record> records, List<RecordColumn> cols, String recordTypeRowIdentifier) {
		return records.stream().map(r -> getInsertArgs(r, cols, recordTypeRowIdentifier)).toList();
	}

	private Object getValueForSql(Object attVal, DataTypeMapping typeMapping) {
		if (Objects.isNull(attVal)) {
			return null;
		}
		if (RelationUtils.isRelationValue(attVal) && typeMapping == DataTypeMapping.RELATION) {
			return RelationUtils.getRelationValue(attVal);
		}
		if (attVal instanceof String sVal) {
			if (stringIsCompatibleWithType(typeMapping == DataTypeMapping.NUMBER, inferer::isNumericValue, sVal)) {
				return new BigDecimal(sVal);
			}
			if (stringIsCompatibleWithType(typeMapping == DataTypeMapping.BOOLEAN, inferer::isValidBoolean, sVal)) {
				return Boolean.parseBoolean(sVal);
			}
			if (stringIsCompatibleWithType(typeMapping == DataTypeMapping.DATE, inferer::isValidDate, sVal)) {
				return LocalDate.parse(sVal, DateTimeFormatter.ISO_LOCAL_DATE);
			}
			if (stringIsCompatibleWithType(typeMapping == DataTypeMapping.DATE_TIME, inferer::isValidDateTime, sVal)) {
				return LocalDateTime.parse(sVal, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
			}
		}
		if(attVal instanceof Map<?,?>){
			try {
				return objectMapper.writeValueAsString(attVal);
			} catch (JsonProcessingException e) {
				LOGGER.error("Could not serialize Map to json string", e);
				throw new RuntimeException(e);
			}
		}
		if(typeMapping.isArrayType()){
			return getArrayValues(attVal, typeMapping);
		}
		if(attVal instanceof List<?> list && typeMapping == DataTypeMapping.STRING){
			return "{"+ list.stream().map(Object::toString).collect(Collectors.joining(",")) +"}";
		}
		return attVal;
	}

	private Object getArrayValues(Object attVal, DataTypeMapping typeMapping) {
		if (attVal instanceof List<?> valAsList) {
			return getListAsArray(valAsList, typeMapping);
		}
		return inferer.getArrayOfType(attVal.toString(), typeMapping.getJavaArrayTypeForDbWrites());
	}

	private Object[] getListAsArray(List<?> attVal, DataTypeMapping typeMapping) {
		switch (typeMapping){
			case ARRAY_OF_STRING, ARRAY_OF_RELATION, ARRAY_OF_DATE, ARRAY_OF_DATE_TIME, ARRAY_OF_NUMBER, EMPTY_ARRAY:
				return attVal.stream().map(Object::toString).toList().toArray(new String[0]);
			case ARRAY_OF_BOOLEAN:
				//accept all casings of True and False if they're strings
				return attVal.stream().map(Object::toString).map(String::toLowerCase)
						.map(Boolean::parseBoolean).toList().toArray(new Boolean[0]);
			default:
				throw new IllegalArgumentException("Unhandled array type " + typeMapping);
		}

	}

	private boolean stringIsCompatibleWithType(boolean typesMatch,
											   Predicate<String> typeCheckPredicate, String attVal) {
		return typesMatch && typeCheckPredicate.test(attVal);
	}

	private Object[] getInsertArgs(Record toInsert, List<RecordColumn> cols, String recordTypeRowIdentifier) {
		Object[] row = new Object[cols.size()];
		int i = 0;
		for (RecordColumn col : cols) {
			String colName = col.colName();
			if (colName.equals(recordTypeRowIdentifier)) {
				row[i++] = toInsert.getId();
			} else {
				row[i++] = getValueForSql(toInsert.getAttributeValue(colName), col.typeMapping());
			}
		}
		return row;
	}

	private List<Object[]> getJoinInsertBatchArgs(List<RelationValue> relations) {
		return relations.stream().map(r -> new Object[]{r.fromRecord().getId(), r.toRecord().getId()}).toList();
	}

	private String genInsertStatement(UUID instanceId, RecordType recordType, List<RecordColumn> schema, String recordTypeIdenifier) {
		List<String> colNames = schema.stream().map(RecordColumn::colName).toList();
		List<DataTypeMapping> colTypes = schema.stream().map(RecordColumn::typeMapping).toList();
		return "insert into " + getQualifiedTableName(recordType, instanceId) + "(" + getInsertColList(colNames)
				+ ") values (" + getInsertParamList(colTypes) + ") " + "on conflict (" + quote(recordTypeIdenifier) + ") "
				+ (schema.size() == 1 ? "do nothing" : "do update set " + genColUpsertUpdates(colNames, recordTypeIdenifier));
	}

	private String genJoinInsertStatement(UUID instanceId, Relation relation, RecordType recordType) {
		String fromCol = getFromColumnName(recordType);
		String toCol = getToColumnName(relation.relationRecordType());
		String columnDefs =  " (" + quote(fromCol) + ", " + quote(toCol) + ")";
		return "insert into " + getQualifiedJoinTableName(instanceId, relation.relationColName(), recordType) + columnDefs
				+ " values (?,?) ";
	}


	private String getInsertParamList(List<DataTypeMapping> colTypes) {
		return colTypes.stream().map(DataTypeMapping::getWritePlaceholder)
				.collect(Collectors.joining(", "));
	}

	private String getInsertColList(List<String> existingTableSchema) {
		return existingTableSchema.stream().map(this::quote).collect(Collectors.joining(", "));
	}

	@SuppressWarnings("squid:S2077")
	public void batchDelete(UUID instanceId, RecordType recordType, List<Record> records) {
		List<String> recordIds = records.stream().map(Record::getId).toList();
		try {
			List<Relation> relationArrayCols = getRelationArrayCols(instanceId, recordType);
			if (!relationArrayCols.isEmpty()){
				//Remove values from join table first
				for (Relation rel : relationArrayCols) {
					namedTemplate.getJdbcTemplate().batchUpdate(
							"delete from" + getQualifiedJoinTableName(instanceId, rel.relationColName(), recordType) + " where " + quote(getFromColumnName(recordType)) + " = ?",
							new BatchPreparedStatementSetter() {
								@Override
								public void setValues(PreparedStatement ps, int i) throws SQLException {
									ps.setString(1, recordIds.get(i));
								}

								@Override
								public int getBatchSize() {
									return recordIds.size();
								}
							});
				}
			}

			int[] rowCounts = namedTemplate.getJdbcTemplate().batchUpdate(
					"delete from" + getQualifiedTableName(recordType, instanceId) + " where " + quote(cachedQueryDao.getPrimaryKeyColumn(recordType, instanceId)) + " = ?",
					new BatchPreparedStatementSetter() {
						@Override
						public void setValues(PreparedStatement ps, int i) throws SQLException {
							ps.setString(1, recordIds.get(i));
						}

						@Override
						public int getBatchSize() {
							return recordIds.size();
						}
					});
			List<String> recordErrors = new ArrayList<>();
			for (int i = 0; i < rowCounts.length; i++) {
				if (rowCounts[i] != 1) {
					recordErrors.add("record id " + recordIds.get(i) + " does not exist in " + recordType.getName());
				}
			}
			if (!recordErrors.isEmpty()) {
				throw new BatchDeleteException(recordErrors);
			}
		} catch (DataIntegrityViolationException e) {
			if (e.getRootCause()instanceof SQLException sqlEx) {
				checkForTableRelation(sqlEx);
			}
			throw e;
		}
	}

	private class RecordRowMapper implements RowMapper<Record> {

		private final RecordType recordType;

		private final Map<String, RecordType> referenceColToTable;

		private final ObjectMapper objectMapper;

		private final Map<String, DataTypeMapping> schema;

		private final String primaryKeyColumn;

		public RecordRowMapper(RecordType recordType, ObjectMapper objectMapper, UUID instanceId){
			this.recordType = recordType;
			this.objectMapper = objectMapper;
			this.schema = RecordDao.this.getExistingTableSchemaLessPrimaryKey(instanceId, recordType);
			this.primaryKeyColumn = cachedQueryDao.getPrimaryKeyColumn(recordType, instanceId);
			this.referenceColToTable = RecordDao.this.getRelationColumnsByName(RecordDao.this.getRelationCols(instanceId, recordType));
		}

		@Override
		public Record mapRow(ResultSet rs, int rowNum) throws SQLException{
			try {
				return new Record(rs.getString(primaryKeyColumn), recordType, getAttributes(rs));
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		}

		private RecordAttributes getAttributes(ResultSet rs) throws JsonProcessingException {
			try {
				ResultSetMetaData metaData = rs.getMetaData();
				RecordAttributes attributes = RecordAttributes.empty(primaryKeyColumn);

				for (int j = 1; j <= metaData.getColumnCount(); j++) {
					String columnName = metaData.getColumnName(j);
					if (columnName.equals(primaryKeyColumn)) {
						attributes.putAttribute(primaryKeyColumn, rs.getString(primaryKeyColumn));
						continue;
					}
					if (referenceColToTable.size() > 0 && referenceColToTable.containsKey(columnName)
							&& rs.getString(columnName) != null) {
						attributes.putAttribute(columnName, RelationUtils
								.createRelationString(referenceColToTable.get(columnName), rs.getString(columnName)));
					} else {
						attributes.putAttribute(columnName, getAttributeValueForType(rs.getObject(columnName), schema.get(columnName)));
					}
				}
				return attributes;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

		private Object getAttributeValueForType(Object object, DataTypeMapping typeMapping) throws SQLException, JsonProcessingException {
			if(object == null){
				return null;
			}
			if (object instanceof java.sql.Date date && typeMapping == DataTypeMapping.DATE) {
				return date.toLocalDate();
			}
			if (object instanceof java.sql.Timestamp ts && typeMapping == DataTypeMapping.DATE_TIME) {
				return ts.toLocalDateTime();
			}
			if(typeMapping.isArrayType() && object instanceof PgArray pgArray){
				return getArrayValue(pgArray.getArray(), typeMapping);
			}
			if(typeMapping == DataTypeMapping.JSON){
				return objectMapper.readValue(object.toString(), new TypeReference<Map<String, Object>>(){});
			}
			return object;
		}

		private Object getArrayValue(Object object, DataTypeMapping typeMapping) {
			if(typeMapping == DataTypeMapping.ARRAY_OF_DATE_TIME){
				return convertToLocalDateTime(object);
			} else if(typeMapping == DataTypeMapping.ARRAY_OF_DATE){
				return convertToLocalDate(object);
			}
			return object;
		}

		private LocalDateTime[] convertToLocalDateTime(Object object) {
			Timestamp[] tzArray = (Timestamp[]) object;
			LocalDateTime[] result = new LocalDateTime[tzArray.length];
			for (int i = 0; i < tzArray.length; i++) {
				result[i] = tzArray[i].toLocalDateTime();
			}
			return result;
		}
		private LocalDate[] convertToLocalDate(Object object) {
			Date[] tzArray = (Date[]) object;
			LocalDate[] result = new LocalDate[tzArray.length];
			for (int i = 0; i < tzArray.length; i++) {
				result[i] = tzArray[i].toLocalDate();
			}
			return result;
		}

	}

	public Optional<Record> getSingleRecord(UUID instanceId, RecordType recordType, String recordId) {
		try {
			return Optional.ofNullable(namedTemplate.queryForObject(
					"select * from " + getQualifiedTableName(recordType, instanceId) + " where " + quote(cachedQueryDao.getPrimaryKeyColumn(recordType, instanceId))
							+ " = :recordId",
					new MapSqlParameterSource(RECORD_ID_PARAM, recordId), new RecordRowMapper(recordType,objectMapper, instanceId)));
		} catch (EmptyResultDataAccessException e) {
			return Optional.empty();
		}
	}

	public String getPrimaryKeyColumn(RecordType type, UUID instanceId){
		return cachedQueryDao.getPrimaryKeyColumn(type, instanceId);
	}

	public boolean recordExists(UUID instanceId, RecordType recordType, String recordId) {
		return Boolean.TRUE
				.equals(namedTemplate.queryForObject(
						"select exists(select * from " + getQualifiedTableName(recordType, instanceId) + " where "
								+ quote(cachedQueryDao.getPrimaryKeyColumn(recordType, instanceId)) + " = :recordId)",
						new MapSqlParameterSource(RECORD_ID_PARAM, recordId), Boolean.class));
	}

	public List<RecordType> getAllRecordTypes(UUID instanceId) {
		return namedTemplate.queryForList(
				"select tablename from pg_tables WHERE schemaname = :workspaceSchema and tablename not like 'sys_%' order by tablename",
				new MapSqlParameterSource("workspaceSchema", instanceId.toString()), RecordType.class);
	}

	Map<String, RecordType> getRelationColumnsByName(List<Relation> referenceCols) {
		return referenceCols.stream()
				.collect(Collectors.toMap(Relation::relationColName, Relation::relationRecordType));
	}

	/**
	 * In order for @CacheEvict to function properly, it needs to be invoked outside
	 * of this class.  Callers keep that in mind :)
	 */
	@CacheEvict(value = PRIMARY_KEY_COLUMN_CACHE, key = "{ #recordType.name, #instanceId.toString()}")
	public void deleteRecordType(UUID instanceId, RecordType recordType) {
		List<Relation> relationArrayCols = getRelationArrayCols(instanceId, recordType);
		if (!relationArrayCols.isEmpty()){
			for (Relation rel : relationArrayCols){
				namedTemplate.getJdbcTemplate().update("drop table " + getQualifiedJoinTableName(instanceId, rel.relationColName(), recordType));
			}
		}
		try {
			namedTemplate.getJdbcTemplate().update("drop table " + getQualifiedTableName(recordType, instanceId));
		} catch (DataAccessException e) {
			if (e.getRootCause()instanceof SQLException sqlEx) {
				checkForTableRelation(sqlEx);
			}
			throw e;
		}
	}
}
