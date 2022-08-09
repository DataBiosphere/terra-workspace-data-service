package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RefUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.EntitySystemColumn;
import org.databiosphere.workspacedataservice.service.model.MissingReferencedTableException;
import org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference;
import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.databiosphere.workspacedataservice.shared.model.EntityAttributes;
import org.databiosphere.workspacedataservice.shared.model.EntityId;
import org.databiosphere.workspacedataservice.shared.model.EntityType;
import org.postgresql.util.PGobject;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static org.databiosphere.workspacedataservice.service.model.EntitySystemColumn.ENTITY_ID;
import static org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference.ENTITY_NAME_KEY;
import static org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference.ENTITY_TYPE_KEY;

@Repository
public class EntityDao {

    private final NamedParameterJdbcTemplate namedTemplate;

    public EntityDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    public boolean workspaceSchemaExists(UUID workspaceId){
        return Boolean.TRUE.equals(namedTemplate.queryForObject("select exists(select from information_schema.schemata WHERE schema_name = :workspaceSchema)",
                new MapSqlParameterSource("workspaceSchema", workspaceId.toString()), Boolean.class));
    }

    public void createSchema(UUID workspaceId){
        namedTemplate.getJdbcTemplate().update("create schema \"" + workspaceId.toString() +"\"");
    }

    public boolean entityTypeExists(UUID workspaceId, String entityType){
        return Boolean.TRUE.equals(namedTemplate.queryForObject("select exists(select from pg_tables where schemaname = :workspaceId AND tablename  = :entityType)",
                new MapSqlParameterSource(Map.of("workspaceId", workspaceId.toString(), "entityType", entityType)), Boolean.class));
    }

    public void createEntityType(UUID workspaceId, Map<String, DataTypeMapping> tableInfo, String tableName, Set<SingleTenantEntityReference> referencedEntityTypes) throws MissingReferencedTableException {
        String columnDefs = genColumnDefs(tableInfo);
        namedTemplate.getJdbcTemplate().update("create table " + getQualifiedTableName(tableName, workspaceId) + "( " + columnDefs + ")");
        for (SingleTenantEntityReference referencedEntityType : referencedEntityTypes) {
            addForeignKeyForReference(tableName, referencedEntityType.getReferencedEntityType().getName(), workspaceId, referencedEntityType.getReferenceColName());
        }
    }

    private String getQualifiedTableName(String entityType, UUID workspaceId){
        return "\"" + workspaceId.toString() + "\".\"" + entityType + "\"";
    }

    public Map<String, DataTypeMapping> getExistingTableSchema(UUID workspaceId, String tableName){
        MapSqlParameterSource params = new MapSqlParameterSource("workspaceId", workspaceId.toString());
        params.addValue("tableName", tableName);
        return namedTemplate.query("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_schema = :workspaceId " +
                "and table_name = :tableName", params, rs -> {
            Map<String, DataTypeMapping> result = new HashMap<>();
            while(rs.next()){
                result.put(rs.getString("column_name"), DataTypeMapping.fromPostgresType(rs.getString("data_type")));
            }
            return result;
        });
    }

    public void addColumn(UUID workspaceId, String tableName, String columnName, DataTypeMapping colType){
        namedTemplate.getJdbcTemplate().update("alter table "+ getQualifiedTableName(tableName, workspaceId) + " add column \"" + columnName + "\" " + colType.getPostgresType());
    }

    public void changeColumn(UUID workspaceId, String tableName, String columnName, DataTypeMapping newColType){
        namedTemplate.getJdbcTemplate().update("alter table " + getQualifiedTableName(tableName, workspaceId) + " alter column \"" + columnName + "\" TYPE " + newColType.getPostgresType());
    }

    private String genColumnDefs(Map<String, DataTypeMapping> tableInfo) {
        return ENTITY_ID.getColumnName() + " text primary key "
                + (tableInfo.size() > 0 ? ", " + tableInfo.entrySet().stream().map(e -> "\"" + e.getKey() + "\" " + e.getValue().getPostgresType()).collect(Collectors.joining(", ")) : "");
    }

    public void batchUpsert(UUID workspaceId, String entityType, List<Entity> entities, LinkedHashMap<String, DataTypeMapping> schema){
        schema.put(ENTITY_ID.getColumnName(), DataTypeMapping.STRING);
        namedTemplate.getJdbcTemplate().batchUpdate(genInsertStatement(workspaceId, entityType, schema),
                getInsertBatchArgs(entities, schema.keySet()));
    }
    //The expectation is that the entity type already matches the schema and attributes given, as that's dealt with earlier in the code.
    public void createSingleEntity(UUID workspaceId, String entityType, Entity entity, LinkedHashMap<String, DataTypeMapping> schema){
        schema.put(ENTITY_ID.getColumnName(), DataTypeMapping.STRING);
        namedTemplate.getJdbcTemplate().update(genInsertStatement(workspaceId, entityType, schema),
                getInsertArgs(entity, schema.keySet()));
    }

    public void addForeignKeyForReference(String entityType, String referencedEntityType, UUID workspaceId, String referenceColName) throws MissingReferencedTableException {
        try {
            String addFk = "alter table " + getQualifiedTableName(entityType, workspaceId) + " add foreign key (\"" + referenceColName + "\") " +
                    "references " + getQualifiedTableName(referencedEntityType, workspaceId);
            namedTemplate.getJdbcTemplate().execute(addFk);
        } catch (DataAccessException e) {
            if(e.getRootCause() instanceof SQLException){
                SQLException sqlEx = (SQLException) e.getRootCause();
                if(sqlEx != null && sqlEx.getSQLState() != null && sqlEx.getSQLState().equals("42P01")){
                    throw new MissingReferencedTableException();
                }
                throw e;
            }
        }
    }

    public List<SingleTenantEntityReference> getReferenceCols(UUID workspaceId, String tableName){
        return namedTemplate.query("SELECT kcu.column_name, ccu.table_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu " +
                        "ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema " +
                        "JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema " +
                        "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = :workspace AND tc.table_name= :tableName",
                Map.of("workspace", workspaceId.toString(), "tableName", tableName), (rs, rowNum) ->
                        new SingleTenantEntityReference(rs.getString("column_name"), new EntityType(rs.getString("table_name"))));
    }

    private String genColUpsertUpdates(Set<String> cols) {
        return cols.stream().filter(c -> !ENTITY_ID.getColumnName().equals(c)).map(c -> "\"" + c + "\"" + " = excluded.\"" + c + "\"").collect(Collectors.joining(", "));
    }

    private List<Object[]> getInsertBatchArgs(List<Entity> entities, Set<String> colNames) {
        List<Object[]> result = new ArrayList<>();
        for (Entity entity : entities) {
            result.add(getInsertArgs(entity, colNames));
        }
        return result;
    }

    private Object getValueForSql(Object attVal) {
        if(RefUtils.isReferenceValue(attVal)) {
            return RefUtils.getRefValue(attVal);
        }
        DataTypeInferer inferer = new DataTypeInferer();

        DataTypeMapping dataTypeMapping = inferer.inferType(attVal);

        switch (dataTypeMapping){
            case DATE -> {
                return LocalDate.parse(attVal.toString(), DateTimeFormatter.ISO_LOCAL_DATE);
            }
            case DATE_TIME -> {
                return LocalDateTime.parse(attVal.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            }
        }
        return attVal;
    }

    private Object[] getInsertArgs(Entity entity, Set<String> colNames) {
            Object[] row = new Object[colNames.size()];
            int i = 0;
            for (String col : colNames) {
                if(col.equals(ENTITY_ID.getColumnName())){
                    row[i++] = entity.getName().getEntityIdentifier();
                } else {
                    Object attVal = entity.getAttributes().getAttributes().get(col);
                    row[i++] = attVal == null ? null : getValueForSql(attVal);
//                    row[i++] = getValueForSql(attVal);
                }
            }
            return row;
    }

    private String genInsertStatement(UUID workspaceId, String entityType, LinkedHashMap<String, DataTypeMapping> schema) {
        return "insert into " + getQualifiedTableName(entityType, workspaceId) + "(" +
                getInsertColList(schema.keySet()) + ") values (" + getInsertParamList(schema.values()) +") " +
                "on conflict (" + ENTITY_ID.getColumnName() +") " + (schema.keySet().size() == 1 ? "do nothing" : "do update set " + genColUpsertUpdates(schema.keySet()));
    }

    private String getInsertParamList(Collection<DataTypeMapping> existingTableSchema) {
        return existingTableSchema.stream().map(m -> m.getPostgresType().equalsIgnoreCase("jsonb") ? "? :: jsonb" : "?").collect(Collectors.joining(", "));
    }

    private String getInsertColList(Set<String> existingTableSchema) {
        return existingTableSchema.stream().map(col ->"\"" + col + "\"").collect(Collectors.joining(", "));
    }

    private class EntityRowMapper implements RowMapper<Entity> {
        private final String entityType;

        private final Map<String, String> referenceColToTable;

        private EntityRowMapper(String entityType, Map<String, String> referenceColToTable) {
            this.entityType = entityType;
            this.referenceColToTable = referenceColToTable;
        }

        @Override
        public Entity mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Entity(new EntityId(rs.getString(ENTITY_ID.getColumnName())), new EntityType(entityType), new EntityAttributes(getAttributes(rs)));
        }

        private Map<String, Object> getAttributes(ResultSet rs) {
            try {
                ResultSetMetaData metaData = rs.getMetaData();
                Map<String, Object> attributes = new HashMap<>();
                Set<String> systemCols = Arrays.stream(EntitySystemColumn.values()).map(EntitySystemColumn::getColumnName).collect(Collectors.toSet());
                for (int j = 0; j < metaData.getColumnCount(); j++) {
                    String columnName = metaData.getColumnName(j+1);
                    if (systemCols.contains(columnName)) {
                        continue;
                    }
                    if(referenceColToTable.size() > 0 && referenceColToTable.containsKey(columnName)){
                        Map<String, String> refMap = new HashMap<>();
                        refMap.put(ENTITY_TYPE_KEY, referenceColToTable.get(columnName));
                        refMap.put(ENTITY_NAME_KEY, rs.getString(columnName));
                        attributes.put(columnName, refMap);
                    } else {
                        Object object = rs.getObject(columnName);
                        attributes.put(columnName, object instanceof PGobject ? ((PGobject)object).getValue() : object);
                    }
                }
                return attributes;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Entity getSingleEntity(UUID instanceId, EntityType entityType, EntityId entityId, List<SingleTenantEntityReference> referenceCols){
        Map<String, String> refColMapping = new HashMap<>();
        referenceCols.forEach(rc -> refColMapping.put(rc.getReferenceColName(), rc.getReferencedEntityType().getName()));
        try {
            return namedTemplate.queryForObject("select * from " + getQualifiedTableName(entityType.getName(), instanceId) + " where " +
                    ENTITY_ID.getColumnName() + " = :entityId", new MapSqlParameterSource("entityId", entityId.getEntityIdentifier()),
                    new EntityRowMapper(entityType.getName(), refColMapping));
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    /**
     * Remove all existing attributes on entity and replace with new values
     * @param entity - which entity to update
     * @param newAttributes - attribute values to replace
     * @param instanceId
     */
    public void replaceAllAttributes(Entity entity, EntityAttributes newAttributes, UUID instanceId){
        //first remove any present attribute values
        removeAllAttributes(entity, instanceId);
        //then add new values
        replaceAttributes(entity, newAttributes, instanceId);
    }

    public void replaceAttributes(Entity entity, EntityAttributes newAttributes, UUID instanceId){
        namedTemplate.getJdbcTemplate().update("update " + getQualifiedTableName(entity.getEntityTypeName(), instanceId) + "set " +
                genReplaceAttrUpdates(newAttributes));
    }

    public void removeAllAttributes(Entity entity, UUID instanceId){
        //Make sure we get all the attributes
        Set<String> schema = getExistingTableSchema(instanceId, entity.getEntityTypeName()).keySet();
        //Except this one!
        schema.remove(ENTITY_ID.getColumnName());
        namedTemplate.getJdbcTemplate().update("update " + getQualifiedTableName(entity.getEntityTypeName(), instanceId) + "set " +
                genRemoveAttrUpdates(schema));
    }

    private String genReplaceAttrUpdates(EntityAttributes newAttributes) {
        return newAttributes.getAttributes().entrySet().stream().map(entry ->  entry.getKey() + "='" + entry.getValue() + "'").collect(Collectors.joining(", "));
    }

    private String genRemoveAttrUpdates(Set<String> attributes) {
        return attributes.stream().map(attr ->  attr + "=NULL").collect(Collectors.joining(", "));
    }

}
