package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.service.model.SystemColumn.ENTITY_ID;

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

  public boolean workspaceSchemaExists(UUID workspaceId) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from information_schema.schemata WHERE schema_name = :workspaceSchema)",
            new MapSqlParameterSource("workspaceSchema", workspaceId.toString()),
            Boolean.class));
  }

  public void createSchema(UUID workspaceId) {
    namedTemplate.getJdbcTemplate().update("create schema \"" + workspaceId.toString() + "\"");
  }

  public boolean entityTypeExists(UUID workspaceId, String entityType) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from pg_tables where schemaname = :workspaceId AND tablename  = :entityType)",
            new MapSqlParameterSource(
                Map.of("workspaceId", workspaceId.toString(), "entityType", entityType)),
            Boolean.class));
  }

  public void createEntityType(
      UUID workspaceId,
      Map<String, DataTypeMapping> tableInfo,
      String tableName,
      Set<Relation> referencedEntityTypes)
      throws MissingReferencedTableException {
    String columnDefs = genColumnDefs(tableInfo);
    namedTemplate
        .getJdbcTemplate()
        .update(
            "create table "
                + getQualifiedTableName(tableName, workspaceId)
                + "( "
                + columnDefs
                + ")");
    for (Relation referencedEntityType : referencedEntityTypes) {
      addForeignKeyForReference(
          tableName,
          referencedEntityType.referencedRecordType().getName(),
          workspaceId,
          referencedEntityType.referenceColName());
    }
  }

  private String getQualifiedTableName(String entityType, UUID workspaceId) {
    return "\"" + workspaceId.toString() + "\".\"" + entityType + "\"";
  }

  public Map<String, DataTypeMapping> getExistingTableSchema(UUID workspaceId, String tableName) {
    MapSqlParameterSource params = new MapSqlParameterSource("workspaceId", workspaceId.toString());
    params.addValue("tableName", tableName);
    return namedTemplate.query(
        "select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_schema = :workspaceId "
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
      UUID workspaceId, String tableName, String columnName, DataTypeMapping colType) {
    namedTemplate
        .getJdbcTemplate()
        .update(
            "alter table "
                + getQualifiedTableName(tableName, workspaceId)
                + " add column \""
                + columnName
                + "\" "
                + colType.getPostgresType());
  }

  public void changeColumn(
      UUID workspaceId, String tableName, String columnName, DataTypeMapping newColType) {
    namedTemplate
        .getJdbcTemplate()
        .update(
            "alter table "
                + getQualifiedTableName(tableName, workspaceId)
                + " alter column \""
                + columnName
                + "\" TYPE "
                + newColType.getPostgresType());
  }

  private String genColumnDefs(Map<String, DataTypeMapping> tableInfo) {
    return ENTITY_ID.getColumnName()
        + " text primary key "
        + (tableInfo.size() > 0
            ? ", "
                + tableInfo.entrySet().stream()
                    .map(e -> "\"" + e.getKey() + "\" " + e.getValue().getPostgresType())
                    .collect(Collectors.joining(", "))
            : "");
  }

  // The expectation is that the entity type already matches the schema and attributes given, as
  // that's dealt with earlier in the code.
  public void batchUpsert(
      UUID workspaceId,
      String entityType,
      List<Record> entities,
      LinkedHashMap<String, DataTypeMapping> schema)
      throws InvalidRelation {
    schema.put(ENTITY_ID.getColumnName(), DataTypeMapping.STRING);
    try {
      namedTemplate
          .getJdbcTemplate()
          .batchUpdate(
              genInsertStatement(workspaceId, entityType, schema),
              getInsertBatchArgs(entities, schema.keySet()));
    } catch (DataAccessException e) {
      if (e.getRootCause() instanceof SQLException sqlEx) {
        if (sqlEx.getSQLState() != null && sqlEx.getSQLState().equals("23503")) {
          throw new InvalidRelation(
              "It looks like you're trying to reference an entity that does not exist.");
        }
      }
      throw e;
    }
  }

  public void addForeignKeyForReference(
      String entityType, String referencedEntityType, UUID workspaceId, String referenceColName)
      throws MissingReferencedTableException {
    try {
      String addFk =
          "alter table "
              + getQualifiedTableName(entityType, workspaceId)
              + " add foreign key (\""
              + referenceColName
              + "\") "
              + "references "
              + getQualifiedTableName(referencedEntityType, workspaceId);
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

  public List<Relation> getReferenceCols(UUID workspaceId, String tableName) {
    return namedTemplate.query(
        "SELECT kcu.column_name, ccu.table_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu "
            + "ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
            + "JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema "
            + "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = :workspace AND tc.table_name= :tableName",
        Map.of("workspace", workspaceId.toString(), "tableName", tableName),
        (rs, rowNum) ->
            new Relation(
                rs.getString("column_name"), new RecordType(rs.getString("table_name"))));
  }

  private String genColUpsertUpdates(Set<String> cols) {
    return cols.stream()
        .filter(c -> !ENTITY_ID.getColumnName().equals(c))
        .map(c -> "\"" + c + "\"" + " = excluded.\"" + c + "\"")
        .collect(Collectors.joining(", "));
  }

  private List<Object[]> getInsertBatchArgs(List<Record> entities, Set<String> colNames) {
    List<Object[]> result = new ArrayList<>();
    for (Record record : entities) {
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
      if (col.equals(ENTITY_ID.getColumnName())) {
        row[i++] = record.getName().getEntityIdentifier();
      } else {
        Object attVal = record.getAttributes().getAttributes().get(col);
        row[i++] = getValueForSql(attVal);
      }
    }
    return row;
  }

  private String genInsertStatement(
      UUID workspaceId, String entityType, LinkedHashMap<String, DataTypeMapping> schema) {
    return "insert into "
        + getQualifiedTableName(entityType, workspaceId)
        + "("
        + getInsertColList(schema.keySet())
        + ") values ("
        + getInsertParamList(schema.values())
        + ") "
        + "on conflict ("
        + ENTITY_ID.getColumnName()
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

  private class EntityRowMapper implements RowMapper<Record> {
    private final String entityType;

    private final Map<String, String> referenceColToTable;

    private EntityRowMapper(String entityType, Map<String, String> referenceColToTable) {
      this.entityType = entityType;
      this.referenceColToTable = referenceColToTable;
    }

    @Override
    public Record mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new Record(
          new RecordId(rs.getString(ENTITY_ID.getColumnName())),
          new RecordType(entityType),
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

  public Record getSingleEntity(
      UUID instanceId,
      RecordType recordType,
      RecordId recordId,
      List<Relation> referenceCols) {
    Map<String, String> refColMapping = new HashMap<>();
    referenceCols.forEach(
        rc -> refColMapping.put(rc.referenceColName(), rc.referencedRecordType().getName()));
    try {
      return namedTemplate.queryForObject(
          "select * from "
              + getQualifiedTableName(recordType.getName(), instanceId)
              + " where "
              + ENTITY_ID.getColumnName()
              + " = :entityId",
          new MapSqlParameterSource("entityId", recordId.getEntityIdentifier()),
          new EntityRowMapper(recordType.getName(), refColMapping));
    } catch (EmptyResultDataAccessException e) {
      return null;
    }
  }
}
