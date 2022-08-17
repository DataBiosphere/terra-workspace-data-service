package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.service.model.SystemColumn.RECORD_ID;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.*;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.postgresql.util.PGobject;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RecordDao {

  private final NamedParameterJdbcTemplate namedTemplate;

  public RecordDao(NamedParameterJdbcTemplate namedTemplate) {
    this.namedTemplate = namedTemplate;
  }

  public boolean instanceSchemaExists(UUID instanceId) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from information_schema.schemata WHERE schema_name = :workspaceSchema)",
            new MapSqlParameterSource("workspaceSchema", instanceId.toString()),
            Boolean.class));
  }

  public void createSchema(UUID instanceId) {
    namedTemplate.getJdbcTemplate().update("create schema \"" + instanceId.toString() + "\"");
  }

  public boolean recordTypeExists(UUID instanceId, String recordType) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from pg_tables where schemaname = :instanceId AND tablename  = :recordType)",
            new MapSqlParameterSource(
                Map.of("instanceId", instanceId.toString(), "recordType", recordType)),
            Boolean.class));
  }

  public void createRecordType(
      UUID instanceId,
      Map<String, DataTypeMapping> tableInfo,
      String tableName,
      Set<Relation> relations)
      throws MissingReferencedTableException {
    String columnDefs = genColumnDefs(tableInfo);
    namedTemplate
        .getJdbcTemplate()
        .update(
            "create table "
                + getQualifiedTableName(tableName, instanceId)
                + "( "
                + columnDefs
                + ")");
    for (Relation relation : relations) {
      addForeignKeyForReference(
          tableName,
          relation.relationRecordType().getName(),
          instanceId,
          relation.relationColName());
    }
  }

  private String getQualifiedTableName(String recordType, UUID instanceId) {
    return "\"" + instanceId.toString() + "\".\"" + recordType + "\"";
  }

  public Map<String, DataTypeMapping> getExistingTableSchema(UUID instanceId, String tableName) {
    MapSqlParameterSource params = new MapSqlParameterSource("instanceId", instanceId.toString());
    params.addValue("tableName", tableName);
    return namedTemplate.query(
        "select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_schema = :instanceId "
            + "and table_name = :tableName",
        params,
        rs -> {
          Map<String, DataTypeMapping> result = new HashMap<>();
          while (rs.next()) {
            result.put(
                rs.getString("column_name"),
                DataTypeMapping.fromPostgresType(rs.getString("data_type")));
          }
          return result;
        });
  }

  public void addColumn(
      UUID instanceId, String tableName, String columnName, DataTypeMapping colType) {
    namedTemplate
        .getJdbcTemplate()
        .update(
            "alter table "
                + getQualifiedTableName(tableName, instanceId)
                + " add column \""
                + columnName
                + "\" "
                + colType.getPostgresType());
  }

  public void changeColumn(
      UUID instanceId, String tableName, String columnName, DataTypeMapping newColType) {
    namedTemplate
        .getJdbcTemplate()
        .update(
            "alter table "
                + getQualifiedTableName(tableName, instanceId)
                + " alter column \""
                + columnName
                + "\" TYPE "
                + newColType.getPostgresType());
  }

  private String genColumnDefs(Map<String, DataTypeMapping> tableInfo) {
    return RECORD_ID.getColumnName()
        + " text primary key "
        + (tableInfo.size() > 0
            ? ", "
                + tableInfo.entrySet().stream()
                    .map(e -> "\"" + e.getKey() + "\" " + e.getValue().getPostgresType())
                    .collect(Collectors.joining(", "))
            : "");
  }

  // The expectation is that the record type already matches the schema and attributes given, as
  // that's dealt with earlier in the code.
  public void batchUpsert(
      UUID instanceId,
      String recordType,
      List<Record> records,
      LinkedHashMap<String, DataTypeMapping> schema)
      throws InvalidRelation {
    schema.put(RECORD_ID.getColumnName(), DataTypeMapping.STRING);
    try {
      namedTemplate
          .getJdbcTemplate()
          .batchUpdate(
              genInsertStatement(instanceId, recordType, schema),
              getInsertBatchArgs(records, schema.keySet()));
    } catch (DataAccessException e) {
      if (e.getRootCause() instanceof SQLException sqlEx) {
        if (sqlEx.getSQLState() != null && sqlEx.getSQLState().equals("23503")) {
          throw new InvalidRelation(
              "It looks like you're trying to reference a record that does not exist.");
        }
      }
      throw e;
    }
  }

  public void addForeignKeyForReference(
      String recordType, String referencedRecordType, UUID instanceId, String relationColName)
      throws MissingReferencedTableException {
    try {
      String addFk =
          "alter table "
              + getQualifiedTableName(recordType, instanceId)
              + " add foreign key (\""
              + relationColName
              + "\") "
              + "references "
              + getQualifiedTableName(referencedRecordType, instanceId);
      namedTemplate.getJdbcTemplate().execute(addFk);
    } catch (DataAccessException e) {
      if (e.getRootCause() instanceof SQLException) {
        SQLException sqlEx = (SQLException) e.getRootCause();
        if (sqlEx != null && sqlEx.getSQLState() != null && sqlEx.getSQLState().equals("42P01")) {
          throw new MissingReferencedTableException();
        }
        throw e;
      }
    }
  }

  public List<Relation> getRelationCols(UUID instanceId, String tableName) {
    return namedTemplate.query(
        "SELECT kcu.column_name, ccu.table_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu "
            + "ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
            + "JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema "
            + "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = :workspace AND tc.table_name= :tableName",
        Map.of("workspace", instanceId.toString(), "tableName", tableName),
        (rs, rowNum) ->
            new Relation(
                rs.getString("column_name"), new RecordType(rs.getString("table_name"))));
  }

  private String genColUpsertUpdates(Set<String> cols) {
    return cols.stream()
        .filter(c -> !RECORD_ID.getColumnName().equals(c))
        .map(c -> "\"" + c + "\"" + " = excluded.\"" + c + "\"")
        .collect(Collectors.joining(", "));
  }

  private List<Object[]> getInsertBatchArgs(List<Record> records, Set<String> colNames) {
    List<Object[]> result = new ArrayList<>();
    for (Record record : records) {
      result.add(getInsertArgs(record, colNames));
    }
    return result;
  }

  private Object getValueForSql(Object attVal) {
    if (RelationUtils.isRelationValue(attVal)) {
      return RelationUtils.getRelationValue(attVal);
    }
    DataTypeInferer inferer = new DataTypeInferer();

    DataTypeMapping dataTypeMapping = inferer.inferType(attVal);

    switch (dataTypeMapping) {
      case DATE -> {
        return LocalDate.parse(attVal.toString(), DateTimeFormatter.ISO_LOCAL_DATE);
      }
      case DATE_TIME -> {
        return LocalDateTime.parse(attVal.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
      }
    }
    return attVal;
  }

  private Object[] getInsertArgs(Record record, Set<String> colNames) {
    Object[] row = new Object[colNames.size()];
    int i = 0;
    for (String col : colNames) {
      if (col.equals(RECORD_ID.getColumnName())) {
        row[i++] = record.getName().getRecordIdentifier();
      } else {
        Object attVal = record.getAttributes().getAttributes().get(col);
        row[i++] = getValueForSql(attVal);
      }
    }
    return row;
  }

  private String genInsertStatement(
      UUID instanceId, String recordType, LinkedHashMap<String, DataTypeMapping> schema) {
    return "insert into "
        + getQualifiedTableName(recordType, instanceId)
        + "("
        + getInsertColList(schema.keySet())
        + ") values ("
        + getInsertParamList(schema.values())
        + ") "
        + "on conflict ("
        + RECORD_ID.getColumnName()
        + ") "
        + (schema.keySet().size() == 1
            ? "do nothing"
            : "do update set " + genColUpsertUpdates(schema.keySet()));
  }

  private String getInsertParamList(Collection<DataTypeMapping> existingTableSchema) {
    return existingTableSchema.stream()
        .map(m -> m.getPostgresType().equalsIgnoreCase("jsonb") ? "? :: jsonb" : "?")
        .collect(Collectors.joining(", "));
  }

  private String getInsertColList(Set<String> existingTableSchema) {
    return existingTableSchema.stream()
        .map(col -> "\"" + col + "\"")
        .collect(Collectors.joining(", "));
  }

  private class RecordRowMapper implements RowMapper<Record> {
    private final String recordType;

    private final Map<String, String> referenceColToTable;

    private RecordRowMapper(String recordType, Map<String, String> referenceColToTable) {
      this.recordType = recordType;
      this.referenceColToTable = referenceColToTable;
    }

    @Override
    public Record mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new Record(
          new RecordId(rs.getString(RECORD_ID.getColumnName())),
          new RecordType(recordType),
          new RecordAttributes(getAttributes(rs)));
    }

    private Map<String, Object> getAttributes(ResultSet rs) {
      try {
        ResultSetMetaData metaData = rs.getMetaData();
        Map<String, Object> attributes = new HashMap<>();
        Set<String> systemCols =
            Arrays.stream(SystemColumn.values())
                .map(SystemColumn::getColumnName)
                .collect(Collectors.toSet());
        for (int j = 0; j < metaData.getColumnCount(); j++) {
          String columnName = metaData.getColumnName(j + 1);
          if (systemCols.contains(columnName)) {
            continue;
          }
          if (referenceColToTable.size() > 0 && referenceColToTable.containsKey(columnName)) {
            attributes.put(columnName, RelationUtils.createRelationString(referenceColToTable.get(columnName), rs.getString(columnName)));
        } else {
            Object object = rs.getObject(columnName);
            attributes.put(
                    columnName, object instanceof PGobject ? ((PGobject) object).getValue() : object);
          }
        }
        return attributes;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Record getSingleRecord(
      UUID instanceId,
      RecordType recordType,
      RecordId recordId,
      List<Relation> referenceCols) {
    Map<String, String> refColMapping = new HashMap<>();
    referenceCols.forEach(
        rc -> refColMapping.put(rc.relationColName(), rc.relationRecordType().getName()));
    try {
      return namedTemplate.queryForObject(
          "select * from "
              + getQualifiedTableName(recordType.getName(), instanceId)
              + " where "
              + RECORD_ID.getColumnName()
              + " = :recordId",
          new MapSqlParameterSource("recordId", recordId.getRecordIdentifier()),
          new RecordRowMapper(recordType.getName(), refColMapping));
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }
}
