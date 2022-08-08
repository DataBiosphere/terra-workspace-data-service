package org.databiosphere.workspacedataservice.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.model.EntityReference;
import org.databiosphere.workspacedataservice.service.model.InvalidEntityReference;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StopWatch;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class EntityDao {

    private final JdbcTemplate template;

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private final ObjectMapper objectMapper;

    private static final Logger LOGGER = LoggerFactory.getLogger(EntityDao.class);

    public EntityDao(JdbcTemplate template, NamedParameterJdbcTemplate namedParameterJdbcTemplate, ObjectMapper objectMapper) {
        this.template = template;
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public List<EntityReference> getReferencesForEntities(List<Entity> entitiesForType, long entityTypeId) {
        List<List<Entity>> chunks = Lists.partition(entitiesForType, 1_000);
        List<EntityReference> result = new ArrayList<>();
        for (List<Entity> chunk : chunks) {
            result.addAll(namedParameterJdbcTemplate.query("select entity_type, entity_name, referenced_entity_type, " +
                            "referenced_entity_name from entity_reference where entity_type = :entityType and entity_name in (:entityNames)",
                    new MapSqlParameterSource(Map.of("entityType", entityTypeId, "entityNames", chunk.stream().map(e -> e.getName().getEntityIdentifier()).collect(Collectors.toSet()))),
                    (rs, rowNum) -> new EntityReference(new EntityId(rs.getString("entity_name")),
                            rs.getLong("entity_type"),
                            rs.getLong("referenced_entity_type"),
                            new EntityId(rs.getString("referenced_entity_name")))));
        }
        return result;
    }

    public void deleteEntities(List<EntityToDelete> entities){
        template.batchUpdate("update entity set deleted = true where entity_type = ? and name = ?", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                EntityToDelete entity = entities.get(i);
                ps.setLong(1, entity.getEntityTypeId());
                ps.setString(2, entity.getName());
            }

            @Override
            public int getBatchSize() {
                return entities.size();
            }
        });
    }

    public boolean areEntitiesReferenced(long entityType, List<String> entities){
        List<List<String>> chunks = Lists.partition(entities, 1_000);
        for (List<String> chunk : chunks) {
            if (namedParameterJdbcTemplate.queryForObject("select count(*) from entity_reference where referenced_entity_type = :entityType and referenced_entity_name in (:entityNames)",
                    new MapSqlParameterSource(Map.of("entityType", entityType, "entityNames", chunk)), Integer.class) > 0) {
                return true;
            }
        }
        return false;
    }

    public UUID getWorkspaceId(String namespace, String name) {
        return template.queryForObject("select id from workspace where name = ? and namespace = ?", UUID.class, name, namespace);
    }


    public void batchUpsert(List<Entity> entities) {
        StopWatch watch = new StopWatch();
        watch.start();
        template.batchUpdate("insert into entity (name, entity_type, deleted, attributes)" +
                "values (?, ?, ?, ? :: jsonb) on conflict (name, entity_type) do update set attributes = excluded.attributes", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Entity entity = entities.get(i);
                //This is an example of a place where we want a string instead of an EntityId
                //Should there be a more direct way to get entityId as a string?
                ps.setString(1, entity.getName().getEntityIdentifier());
                ps.setLong(2, entity.getEntityTypeId());
                ps.setBoolean(3, entity.getDeleted());
                ps.setObject(4, writeAsJson(entity.getAttributes()));
            }

            @Override
            public int getBatchSize() {
                return entities.size();
            }
        });
        watch.stop();
        LOGGER.info("Finished loading entities in {}s", watch.getTotalTimeSeconds());
    }

    private String writeAsJson(Object toWrite){
        try {
            return objectMapper.writeValueAsString(toWrite);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public void saveEntitiesReferenced(List<EntityReference> toAdd) throws InvalidEntityReference {
        try {
            template.batchUpdate("insert into entity_reference (entity_type, entity_name, " +
                    "referenced_entity_type, referenced_entity_name) values (?, ?, ?, ?)", new EntityReferenceStmtSetter(toAdd));
        } catch (DataIntegrityViolationException e) {
            LOGGER.error("Could not save entity refs", e);
            throw new InvalidEntityReference("Invalid entity references detected " + toAdd);
        }
    }

    public Entity getSingleEntity(UUID instanceId, EntityType entityType, EntityId entityId) {
        MapSqlParameterSource params = new MapSqlParameterSource("instanceId", instanceId);
        params.addValue("entityTypeName", entityType.getName());
        params.addValue("entityId", entityId.getEntityIdentifier());
        List<Entity> shouldBeSingleEntity = namedParameterJdbcTemplate.query("select e.name, e.attributes, et.id as entity_type_id from entity e join entity_type et " +
                        "on e.entity_type = et.id where et.workspace_id = :instanceId and et.name = :entityTypeName " +
                        "and e.name = :entityId and deleted = false",
                params, (rs, i) -> new Entity(entityId, entityType, getAttributes(rs.getString("attributes")),
                        rs.getLong("entity_type_id"), false));
        return shouldBeSingleEntity.isEmpty() ? null : shouldBeSingleEntity.get(0);
    }

    private static class EntityReferenceStmtSetter implements BatchPreparedStatementSetter {
        private final List<EntityReference> referencesToRemove;

        public EntityReferenceStmtSetter(List<EntityReference> referencesToRemove) {
            this.referencesToRemove = referencesToRemove;
        }

        @Override
        public void setValues(PreparedStatement ps, int i) throws SQLException {
            EntityReference ref = referencesToRemove.get(i);
            ps.setLong(1, ref.getEntityType());
            ps.setString(2, ref.getEntityName().getEntityIdentifier());
            ps.setLong(3, ref.getReferencedEntityType());
            ps.setString(4, ref.getReferencedEntityName().getEntityIdentifier());
        }

        @Override
        public int getBatchSize() {
            return referencesToRemove.size();
        }
    }

    public List<Entity> getNamedEntities(Long entityTypeId, Set<EntityId> entityNames, String entityTypeName, boolean excludeDeletedEntities) {
        String sql = "select name, attributes, entity_type, deleted from entity where entity_type = :entityType and name in (:entityNames) " + (excludeDeletedEntities ? "and deleted = false" : "");
        return namedParameterJdbcTemplate.query(sql,
                new MapSqlParameterSource(Map.of("entityType", entityTypeId, "entityNames", entityNames.stream().map(EntityId::getEntityIdentifier).collect(Collectors.toSet()))),
                (rs, i) -> new Entity(new EntityId(rs.getString("name")), new EntityType(entityTypeName), getAttributes(rs.getString("attributes")),
                        rs.getLong("entity_type"), rs.getBoolean("deleted")));
    }

    public List<EntityToDelete> getEntitiesToDelete(Long entityTypeId, Set<String> entityNames, String entityTypeName, boolean excludeDeletedEntities) {
        String sql = "select name, entity_type  from entity where entity_type = :entityType and name in (:entityNames) " + (excludeDeletedEntities ? "and deleted = false" : "");
        return namedParameterJdbcTemplate.query(sql,
                new MapSqlParameterSource(Map.of("entityType", entityTypeId, "entityNames", entityNames)),
                (rs, i) -> new EntityToDelete(rs.getString("name"), rs.getLong("entity_type")));
    }

    private EntityAttributes getAttributes(String attrString)  {
        TypeReference<HashMap<String, Object>> typeRef
                = new TypeReference<>() {
        };
        try {
            return new EntityAttributes(objectMapper.readValue(attrString, typeRef));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertWorkspace(String name, String namespace, UUID id){
        template.update("insert into workspace (id, name, namespace) values (?,?,?)", id, name, namespace);
    }

    public long loadEntityType(EntityType entityType, UUID instanceId) {
        GeneratedKeyHolder generatedKeyHolder = new GeneratedKeyHolder();
        template.update(con -> {
            PreparedStatement ps = con.prepareStatement("insert into entity_type (name, workspace_id) values (?, ?)", Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, entityType.getName());
            ps.setObject(2, instanceId);
            return ps;
        }, generatedKeyHolder);
        return (long)generatedKeyHolder.getKeys().get("id");
    }

    public Long getEntityTypeId(UUID instanceId, String entityTypeName){
        try {
            LOGGER.info("Querying for {} and {}", instanceId, entityTypeName);
            return template.queryForObject("select id from entity_type where workspace_id = ? and name = ?",
                    Long.class, instanceId, entityTypeName);
        } catch (EmptyResultDataAccessException e) {
            return -1L;
        }
    }

    public int getTotalEntityCount(long entityTypeId){
        return template.queryForObject("select count(*) from entity where entity_type = ? and deleted = false", Integer.class, entityTypeId);
    }

    public int getFilteredEntityCount(long entityTypeId, String filter){
        StringBuilder sql = new StringBuilder("select count(*) from entity e " +
                "join entity_attribute_info ea on ea.entity_name = e.name " +
                "and ea.entity_type = e.entity_type where e.entity_type = :entityType and e.deleted = false");
        MapSqlParameterSource params = new MapSqlParameterSource("entityType", entityTypeId);
        buildFilterSql(filter, sql, params);
        return namedParameterJdbcTemplate.queryForObject(sql.toString(), params, Integer.class);
    }

    public List<Entity> getSelectedEntities(long entityTypeId, int limit, int offset, String filter, String orderCol,
                                            String sortOrder, List<String> fields){
        StopWatch watch = new StopWatch();
        watch.start();
        StringBuilder builder = new StringBuilder("select name, attributes from entity e ");
        MapSqlParameterSource params = new MapSqlParameterSource("entityType", entityTypeId);
        params.addValues(Map.of("limit", limit, "offset", offset));
        if(StringUtils.isNotBlank(filter)){
            builder.append("join entity_attribute_info ea on ea.entity_name = e.name " +
                    "and ea.entity_type = e.entity_type " +
                    "where e.entity_type = :entityType and e.deleted = false");
            buildFilterSql(filter, builder, params);
        } else {
            builder.append("where e.entity_type = :entityType and e.deleted = false");
        }
        String entityTypeName = getEntityTypeName(entityTypeId);
        builder.append(getOrderByClause(orderCol, sortOrder));
        builder.append(" limit :limit offset :offset");
        List<Entity> result = namedParameterJdbcTemplate.query(builder.toString(), params,
                (rs, i) -> {
                    EntityAttributes attributes = getAttributes(rs.getString("attributes"));
                    if (!CollectionUtils.isEmpty(fields)) {
                        attributes.getAttributes().keySet().retainAll(fields);
                    }
                    return new Entity(new EntityId(rs.getString("name")), new EntityType(entityTypeName), attributes);
                });
        watch.stop();
        LOGGER.info("Total time spent was {}s for {}, {}", watch.getTotalTimeSeconds(), builder, params);
        return result;
    }


    public String getEntityTypeName(long entityTypeId){
        return template.queryForObject("select name from entity_type where id = ?", String.class, entityTypeId);
    }

    private String getOrderByClause(String orderCol, String sortOrder){
        if(orderCol.equals("name")){
            return " order by name " + sortOrder;
        }
        return " order by (attributes->>'"+orderCol+"')::varchar " + sortOrder;
    }


    private void buildFilterSql(String filter, StringBuilder builder, MapSqlParameterSource params) {
        String[] filters = filter.split(" ");
        for (int i = 0; i < filters.length; i++) {
            builder.append(" and ea.entity_attribute_values ilike :filter").append(i);
            params.addValue("filter" + i, "%" + filters[i] + "%");
        }
    }


    public void removeReferences(List<EntityReference> referencesToRemove) {
        template.batchUpdate("delete from entity_reference where entity_type = ? and entity_name = ? " +
                "and referenced_entity_type = ? and referenced_entity_name = ?", new EntityReferenceStmtSetter(referencesToRemove));
    }



}
