package org.databiosphere.workspacedataservice.dao;

import com.google.common.collect.ArrayListMultimap;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.Entity;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Repository
public class SingleTenantDao {

    private final NamedParameterJdbcTemplate namedTemplate;

    public SingleTenantDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    public boolean workspaceSchemaExists(UUID workspaceId){
        return namedTemplate.queryForObject("select exists(select 1 from information_schema.schemata WHERE schema_name = :workspaceSchema)",
                new MapSqlParameterSource("workspaceSchema", workspaceId.toString()), Boolean.class);
    }

    public void createSchema(UUID workspaceId){
        namedTemplate.getJdbcTemplate().update("create schema \"" + workspaceId.toString() +"\"");
    }

    public boolean entityTypeExists(UUID workspaceId, String entityType){
        return namedTemplate.queryForObject("SELECT EXISTS(SELECT FROM pg_tables WHERE schemaname = :workspaceId AND tablename  = :entityType)",
                new MapSqlParameterSource(Map.of("workspaceId", workspaceId.toString(), "entityType", entityType)), Boolean.class);
    }

    public void createEntityType(UUID workspaceId, Map<String, DataTypeMapping> tableInfo, String tableName){
        String columnDefs = genColumnDefs(tableInfo);
        namedTemplate.getJdbcTemplate().update("create table " + getQualifiedTableName(tableName, workspaceId) + "( " + columnDefs + ")");
    }

    private String getQualifiedTableName(String entityType, UUID workspaceId){
        return "\"" + workspaceId.toString() + "\"." + entityType;
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
        namedTemplate.getJdbcTemplate().update("alter table "+ getQualifiedTableName(tableName, workspaceId) + " add column " + columnName + " " + colType.getPostgresType());
    }

    public void changeColumn(UUID workspaceId, String tableName, String columnName, DataTypeMapping newColType){
        namedTemplate.getJdbcTemplate().update("alter table " + getQualifiedTableName(tableName, workspaceId) + " alter column " + columnName + " TYPE " + newColType.getPostgresType());
    }

    private String genColumnDefs(Map<String, DataTypeMapping> tableInfo) {
        return "name text primary key, "
                + tableInfo.entrySet().stream().map(e -> e.getKey() + " " + e.getValue().getPostgresType()).collect(Collectors.joining(", "))
                + ", all_attribute_values text not null";
    }

    public void insertEntities(UUID workspaceId, String entityType, List<Entity> entities, LinkedHashMap<String, DataTypeMapping> existingTableSchema){
        namedTemplate.getJdbcTemplate().batchUpdate(genInsertStatement(workspaceId, entityType, existingTableSchema),
                getInsertBatchArgs(entities, existingTableSchema.keySet()));
    }

    public void updateEntities(UUID workspaceId, String entityType, List<Entity> entities, Map<String, DataTypeMapping> allFields){
        ArrayListMultimap<Set<String>, Entity> entitiesByValuesToUpdate = ArrayListMultimap.create();
        for (Entity entity : entities) {
            entitiesByValuesToUpdate.put(entity.getAttributes().keySet(), entity);
        }
        for(Set<String> cols : entitiesByValuesToUpdate.keySet()){
            namedTemplate.getJdbcTemplate().batchUpdate(generateUpdateSql(entityType, workspaceId, cols, allFields),
                    getUpdateBatchArgs(entitiesByValuesToUpdate.get(cols), cols));
        }
    }

    private List<Object[]> getUpdateBatchArgs(List<Entity> entities, Set<String> cols) {
        List<Object[]> result = new ArrayList<>();
        for (Entity entity : entities) {
            Object[] params = new Object[cols.size()*2+1];
            int i = 0;
            for (String col : cols) {
                params[i++] = entity.getAttributes().get(col);
            }
            for(String col : cols) {
                params[i++] = entity.getAttributes().get(col);
            }
            params[i] = entity.getName();
            result.add(params);
        }
        return result;
    }

    private String generateUpdateSql(String entityType, UUID workspaceId, Set<String> fieldsToUpdate, Map<String, DataTypeMapping> allFields) {
        return "update " + getQualifiedTableName(entityType, workspaceId) + " set " + genColUpdates(fieldsToUpdate, allFields) + ", " +
                "all_attribute_values = concat("+getFieldsConcat(allFields.keySet(), fieldsToUpdate)+") where name = ?";
    }

    private String getFieldsConcat(Set<String> allCols, Set<String> colsToUpdate) {
        return allCols.stream().filter(c -> !c.equals("all_attribute_values")).map(colName ->
            colsToUpdate.contains(colName) ? "?" : "coalesce("+colName+",'')"
        ).collect(Collectors.joining(",' ',"));
    }

    private String genColUpdates(Set<String> cols, Map<String, DataTypeMapping> typeMapping) {
        return cols.stream().map(c -> c + " = ? " + (typeMapping.get(c) == DataTypeMapping.JSON ? ":: jsonb" : "")).collect(Collectors.joining(", "));
    }

    public Set<String> getEntityNames(UUID workspaceId, String entityType){
        return new HashSet<>(namedTemplate.getJdbcTemplate().queryForList("select name from " + getQualifiedTableName(entityType, workspaceId), String.class));
    }

    private List<Object[]> getInsertBatchArgs(List<Entity> entities, Set<String> colNames) {
        List<Object[]> result = new ArrayList<>();
        for (Entity entity : entities) {
            Object[] row = new Object[colNames.size()];
            int i = 0;
            for (String col : colNames) {
                if(col.equals("name")){
                    row[i++] = entity.getName();
                } else if (col.equals("all_attribute_values")) {
                   row[i++] = Stream.concat(Stream.of(entity.getName()), entity.getAttributes().values().stream()).map(Object::toString).collect(Collectors.joining(" "));
                } else {
                    row[i++] = entity.getAttributes().get(col);
                }
            }
            result.add(row);
        }
        return result;
    }

    private String genInsertStatement(UUID workspaceId, String entityType, Map<String, DataTypeMapping> existingTableSchema) {
        return "insert into " + getQualifiedTableName(entityType, workspaceId) + "(" +
                getInsertColList(existingTableSchema.keySet()) + ") values (" + getInsertParamList(existingTableSchema.values()) +")";
    }

    private String getInsertParamList(Collection<DataTypeMapping> existingTableSchema) {
        return existingTableSchema.stream().map(m -> m.getPostgresType().equalsIgnoreCase("jsonb") ? "? :: jsonb" : "?").collect(Collectors.joining(", "));
    }

    private String getInsertColList(Set<String> existingTableSchema) {
        return String.join(", ", existingTableSchema);
    }

    public int getEntityCount(String entityType, UUID workspaceId) {
        return namedTemplate.getJdbcTemplate().queryForObject("select count(*) from " + getQualifiedTableName(entityType, workspaceId), Integer.class);
    }

    public int getFilteredEntityCount(UUID workspaceId, String entityType, String filterCol, String terms) {
        return namedTemplate.getJdbcTemplate().queryForObject("select count(*) from " + getQualifiedTableName(entityType, workspaceId) + " where " + filterCol + " = ?", Integer.class, terms);
    }

    public List<Entity> getSelectedEntities(String entityType, int pageSize, int i, String filterTerms, String sortField,
                                            String sortDirection, List<String> fields, UUID workspaceId, String filterCol) {
//        namedTemplate.getJdbcTemplate().query("select " + getFieldList(fields) + " from "
//                + getQualifiedTableName(entityType, workspaceId) + " where " + filterCol + " = ? order by " + sortField
//                + " " + sortDirection + " limit " + pageSize + " offset " + i, );
        return null;
    }

    private String getFieldList(List<String> fields) {
        return fields.isEmpty() ? "*" : String.join(", ", fields);
    }
}
