package org.databiosphere.workspacedataservice.dao;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.InBoundDataSource;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.exception.BatchDeleteException;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordColumn;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.web.server.ResponseStatusException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;
import static org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException.NameType.ATTRIBUTE;
import static org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException.NameType.RECORD_TYPE;

@Repository
public class RecordDao {

	private final NamedParameterJdbcTemplate namedTemplate;

	private final NamedParameterJdbcTemplate templateForStreaming;

	private final DataTypeInferer inferer;
	public RecordDao(NamedParameterJdbcTemplate namedTemplate,
			@Qualifier("streamingDs") NamedParameterJdbcTemplate templateForStreaming) {
		this.namedTemplate = namedTemplate;
		this.templateForStreaming = templateForStreaming;
		this.inferer = new DataTypeInferer();
	}

	public boolean instanceSchemaExists(UUID instanceId) {
		return Boolean.TRUE.equals(namedTemplate.queryForObject(
				"select exists(select from information_schema.schemata WHERE schema_name = :workspaceSchema)",
				new MapSqlParameterSource("workspaceSchema", instanceId.toString()), Boolean.class));
	}

	public void createSchema(UUID instanceId) {
		namedTemplate.getJdbcTemplate().update("create schema " + quote(instanceId.toString()));
	}

	public boolean recordTypeExists(UUID instanceId, RecordType recordType) {
		return Boolean.TRUE.equals(namedTemplate.queryForObject(
				"select exists(select from pg_tables where schemaname = :instanceId AND tablename  = :recordType)",
				new MapSqlParameterSource(
						Map.of("instanceId", instanceId.toString(), "recordType", recordType.getName())),
				Boolean.class));
	}

	@SuppressWarnings("squid:S2077")
	public void createRecordType(UUID instanceId, Map<String, DataTypeMapping> tableInfo, RecordType recordType,
			Set<Relation> relations) {

		String columnDefs = genColumnDefs(tableInfo);
		try {
			namedTemplate.getJdbcTemplate().update("create table " + getQualifiedTableName(recordType, instanceId)
					+ "( " + columnDefs + (!relations.isEmpty() ? ", " + getFkSql(relations, instanceId) : "") + ")");
		} catch (DataAccessException e) {
			if (e.getRootCause()instanceof SQLException sqlEx) {
				checkForMissingTable(sqlEx);
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
		return namedTemplate.getJdbcTemplate().query(
				"select * from " + getQualifiedTableName(recordType, instanceId) + " order by "
						+ (sortAttribute == null ? RECORD_ID : quote(sortAttribute)) + " " + sortDirection + " limit "
						+ pageSize + " offset " + offset,
				new RecordRowMapper(recordType, getRelationColumnsByName(getRelationCols(instanceId, recordType))));
	}

	public Map<String, DataTypeMapping> getExistingTableSchema(UUID instanceId, RecordType recordType) {
		MapSqlParameterSource params = new MapSqlParameterSource("instanceId", instanceId.toString());
		params.addValue("tableName", recordType.getName());
		params.addValue("recordName", RECORD_ID);
		return namedTemplate
				.query("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_schema = :instanceId "
						+ "and table_name = :tableName and column_name != :recordName", params, rs -> {
							Map<String, DataTypeMapping> result = new HashMap<>();
							while (rs.next()) {
								result.put(rs.getString("column_name"),
										DataTypeMapping.fromPostgresType(rs.getString("data_type")));
							}
							return result;
						});
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

	private String genColumnDefs(Map<String, DataTypeMapping> tableInfo) {
		return RECORD_ID + " text primary key"
				+ (tableInfo.size() > 0
						? ", " + tableInfo.entrySet().stream()
								.map(e -> quote(SqlUtils.validateSqlString(e.getKey(), ATTRIBUTE)) + " "
										+ e.getValue().getPostgresType())
								.collect(Collectors.joining(", "))
						: "");
	}

	private String quote(String toQuote) {
		return "\"" + toQuote + "\"";
	}

	// The expectation is that the record type already matches the schema and
	// attributes given, as
	// that's dealt with earlier in the code.
	public void batchUpsert(UUID instanceId, RecordType recordType, List<Record> records,
			Map<String, DataTypeMapping> schema) {
		List<RecordColumn> schemaAsList = getSchemaWithRowId(schema);
		try {
			namedTemplate.getJdbcTemplate().batchUpdate(genInsertStatement(instanceId, recordType, schemaAsList),
					getInsertBatchArgs(records, schemaAsList));
		} catch (DataAccessException e) {
			if (e.getRootCause()instanceof SQLException sqlEx) {
				checkForMissingRecord(sqlEx);
			}
			throw e;
		}
	}

	private List<RecordColumn> getSchemaWithRowId(Map<String, DataTypeMapping> schema) {
		schema.put(RECORD_ID, DataTypeMapping.STRING);
		return schema.entrySet().stream().map(e -> new RecordColumn(e.getKey(), e.getValue())).toList();
	}

	public void batchUpsertWithErrorCapture(UUID instanceId, RecordType recordType, List<Record> records,
			Map<String, DataTypeMapping> schema) {
		try {
			batchUpsert(instanceId, recordType, records, schema);
		} catch (DataAccessException e) {
			if (isDataMismatchException(e)) {
				Map<String, DataTypeMapping> recordTypeSchemaWithoutId = new HashMap<>(schema);
				recordTypeSchemaWithoutId.remove(RECORD_ID);
				List<String> rowErrors = checkEachRow(records, recordTypeSchemaWithoutId);
				if (!rowErrors.isEmpty()) {
					throw new BatchWriteException(rowErrors);
				}
			}
			throw e;
		}
	}

	private List<String> checkEachRow(List<Record> records, Map<String, DataTypeMapping> recordTypeSchema) {
		List<String> result = new ArrayList<>();
		for (Record rcd : records) {
			Map<String, DataTypeMapping> schemaForRecord = inferer.inferTypes(rcd.getAttributes(),
					InBoundDataSource.JSON);
			if (!schemaForRecord.equals(recordTypeSchema)) {
				MapDifference<String, DataTypeMapping> difference = Maps.difference(schemaForRecord, recordTypeSchema);
				Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap = difference
						.entriesDiffering();
				result.add(convertSchemaDiffToErrorMessage(differenceMap, rcd));
			}
		}
		return result;
	}

	private String convertSchemaDiffToErrorMessage(
			Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap, Record rcd) {
		return differenceMap.keySet().stream()
				.map(attr -> rcd.getId() + "." + attr + " is a " + differenceMap.get(attr).leftValue()
						+ " in the request but is defined as " + differenceMap.get(attr).rightValue()
						+ " in the record type definition for " + rcd.getRecordType())
				.collect(Collectors.joining("\n"));
	}

	private boolean isDataMismatchException(DataAccessException e) {
		return e.getRootCause()instanceof SQLException sqlException && sqlException.getSQLState().equals("42804");
	}

	public boolean deleteSingleRecord(UUID instanceId, RecordType recordType, String recordId) {
		try {
			return namedTemplate.update("delete from " + getQualifiedTableName(recordType, instanceId) + " where "
					+ RECORD_ID + " = :recordId", new MapSqlParameterSource("recordId", recordId)) == 1;
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
				"select * from " + getQualifiedTableName(recordType, instanceId) + " order by " + RECORD_ID,
				new RecordRowMapper(recordType, getRelationColumnsByName(getRelationCols(instanceId, recordType))));
	}

	public String getFkSql(Set<Relation> relations, UUID instanceId) {

		return relations.stream()
				.map(r -> "constraint " + quote("fk_" + SqlUtils.validateSqlString(r.relationColName(), ATTRIBUTE))
						+ " foreign key (" + quote(SqlUtils.validateSqlString(r.relationColName(), ATTRIBUTE))
						+ ") references " + getQualifiedTableName(r.relationRecordType(), instanceId) + "(" + RECORD_ID
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

	@SuppressWarnings("squid:S2077")
	public int countRecords(UUID instanceId, RecordType recordType) {
		return namedTemplate.getJdbcTemplate()
				.queryForObject("select count(*) from " + getQualifiedTableName(recordType, instanceId), Integer.class);
	}

	private String genColUpsertUpdates(List<String> cols) {
		return cols.stream().filter(c -> !RECORD_ID.equals(c)).map(c -> quote(c) + " = excluded." + quote(c))
				.collect(Collectors.joining(", "));
	}

	private List<Object[]> getInsertBatchArgs(List<Record> records, List<RecordColumn> cols) {
		return records.stream().map(r -> getInsertArgs(r, cols)).toList();
	}

	private Object getValueForSql(Object attVal, DataTypeMapping typeMapping) {
		if (Objects.isNull(attVal)) {
			return null;
		}
		if (RelationUtils.isRelationValue(attVal)) {
			return RelationUtils.getRelationValue(attVal);
		}
		if (attVal instanceof String sVal) {
			if (checkForCompatibleTypes(typeMapping, DataTypeMapping.LONG, inferer::isLongValue, sVal)) {
				return Long.parseLong(sVal);
			}
			if (checkForCompatibleTypes(typeMapping, DataTypeMapping.DOUBLE, inferer::isDoubleValue, sVal)) {
				return Double.parseDouble(sVal);
			}
			if (checkForCompatibleTypes(typeMapping, DataTypeMapping.BOOLEAN, inferer::isValidBoolean, sVal)) {
				return Boolean.parseBoolean(sVal);
			}
			if (checkForCompatibleTypes(typeMapping, DataTypeMapping.DATE, inferer::isValidDate, sVal)) {
				return LocalDate.parse(sVal, DateTimeFormatter.ISO_LOCAL_DATE);
			}
			if (checkForCompatibleTypes(typeMapping, DataTypeMapping.DATE_TIME, inferer::isValidDateTime, sVal)) {
				return LocalDateTime.parse(sVal, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
			}
		}
		return attVal;
	}

	private boolean checkForCompatibleTypes(DataTypeMapping attributeType, DataTypeMapping typeMapping,
			Function<String, Boolean> typeCheckFunc, String attVal) {
		return attributeType == typeMapping && typeCheckFunc.apply(attVal);
	}

	private Object[] getInsertArgs(Record toInsert, List<RecordColumn> cols) {
		Object[] row = new Object[cols.size()];
		int i = 0;
		for (RecordColumn col : cols) {
			String colName = col.colName();
			if (colName.equals(RECORD_ID)) {
				row[i++] = toInsert.getId();
			} else {
				row[i++] = getValueForSql(toInsert.getAttributeValue(colName), col.typeMapping());
			}
		}
		return row;
	}

	private String genInsertStatement(UUID instanceId, RecordType recordType, List<RecordColumn> schema) {
		List<String> colNames = schema.stream().map(RecordColumn::colName).toList();
		List<DataTypeMapping> colTypes = schema.stream().map(RecordColumn::typeMapping).toList();
		return "insert into " + getQualifiedTableName(recordType, instanceId) + "(" + getInsertColList(colNames)
				+ ") values (" + getInsertParamList(colTypes) + ") " + "on conflict (" + RECORD_ID + ") "
				+ (schema.size() == 1 ? "do nothing" : "do update set " + genColUpsertUpdates(colNames));
	}

	private String getInsertParamList(List<DataTypeMapping> colTypes) {
		return colTypes.stream().map(type -> type.getPostgresType().equalsIgnoreCase("jsonb") ? "? :: jsonb" : "?")
				.collect(Collectors.joining(", "));
	}

	private String getInsertColList(List<String> existingTableSchema) {
		return existingTableSchema.stream().map(this::quote).collect(Collectors.joining(", "));
	}

	@SuppressWarnings("squid:S2077")
	public void batchDelete(UUID instanceId, RecordType recordType, List<Record> records) {
		List<String> recordIds = records.stream().map(Record::getId).toList();
		try {
			int[] rowCounts = namedTemplate.getJdbcTemplate().batchUpdate(
					"delete from" + getQualifiedTableName(recordType, instanceId) + " where " + RECORD_ID + " = ?",
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

	private record RecordRowMapper(RecordType recordType,
			Map<String, RecordType> referenceColToTable) implements RowMapper<Record> {

		@Override
		public Record mapRow(ResultSet rs, int rowNum) throws SQLException {
			return new Record(rs.getString(RECORD_ID), recordType, getAttributes(rs));
		}

		private RecordAttributes getAttributes(ResultSet rs) {
			try {
				ResultSetMetaData metaData = rs.getMetaData();
				RecordAttributes attributes = RecordAttributes.empty();

				for (int j = 1; j <= metaData.getColumnCount(); j++) {
					String columnName = metaData.getColumnName(j);
					if (columnName.startsWith(RESERVED_NAME_PREFIX)) {
						continue;
					}
					if (referenceColToTable.size() > 0 && referenceColToTable.containsKey(columnName)
							&& rs.getString(columnName) != null) {
						attributes.putAttribute(columnName, RelationUtils
								.createRelationString(referenceColToTable.get(columnName), rs.getString(columnName)));
					} else {
						Object object = rs.getObject(columnName);

						if (object instanceof java.sql.Date date) {
							object = date.toLocalDate();
						} else if (object instanceof java.sql.Timestamp ts) {
							object = ts.toLocalDateTime();
						} else if (object instanceof PGobject pGobject) {
							object = pGobject.getValue();
						}

						attributes.putAttribute(columnName, object);
					}
				}
				return attributes;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public Optional<Record> getSingleRecord(UUID instanceId, RecordType recordType, String recordId,
			List<Relation> referenceCols) {
		Map<String, RecordType> refColMapping = getRelationColumnsByName(referenceCols);
		try {
			return Optional.ofNullable(namedTemplate.queryForObject(
					"select * from " + getQualifiedTableName(recordType, instanceId) + " where " + RECORD_ID
							+ " = :recordId",
					new MapSqlParameterSource("recordId", recordId), new RecordRowMapper(recordType, refColMapping)));
		} catch (EmptyResultDataAccessException e) {
			return Optional.empty();
		}
	}

	public boolean recordExists(UUID instanceId, RecordType recordType, String recordId) {
		return Boolean.TRUE
				.equals(namedTemplate.queryForObject(
						"select exists(select * from " + getQualifiedTableName(recordType, instanceId) + " where "
								+ RECORD_ID + " = :recordId)",
						new MapSqlParameterSource("recordId", recordId), Boolean.class));
	}

	public List<RecordType> getAllRecordTypes(UUID instanceId) {
		return namedTemplate.queryForList(
				"select tablename from pg_tables WHERE schemaname = :workspaceSchema order by tablename",
				new MapSqlParameterSource("workspaceSchema", instanceId.toString()), RecordType.class);
	}

	private static Map<String, RecordType> getRelationColumnsByName(List<Relation> referenceCols) {
		return referenceCols.stream()
				.collect(Collectors.toMap(Relation::relationColName, Relation::relationRecordType));
	}

	public void deleteRecordType(UUID instanceId, RecordType recordType) {
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
