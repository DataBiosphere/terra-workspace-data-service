package org.databiosphere.workspacedataservice.dao;

import bio.terra.common.db.WriteTransaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;

@Repository
public class PostgresInstanceDao implements InstanceDao {

    @Value("${spring.datasource.username}")
    private String wdsDbUser;

    private final NamedParameterJdbcTemplate namedTemplate;

    public PostgresInstanceDao(NamedParameterJdbcTemplate namedTemplate) {
        this.namedTemplate = namedTemplate;
    }

    @Override
    public boolean instanceSchemaExists(UUID instanceId) {
        return Boolean.TRUE.equals(namedTemplate.queryForObject(
                "select exists(select from sys_wds.instance WHERE id = :instanceId)",
                new MapSqlParameterSource("instanceId", instanceId), Boolean.class));
    }

    @Override
    public List<UUID> listInstanceSchemas() {
        return namedTemplate.getJdbcTemplate().queryForList(
                "select id from sys_wds.instance order by id",
                UUID.class
        );
    }

    @Override
    @WriteTransaction
    @SuppressWarnings("squid:S2077") // since instanceId must be a UUID, it is safe to use inline
    public void createSchema(UUID instanceId) {
        namedTemplate.getJdbcTemplate().update("insert into sys_wds.instance(id) values (?)", instanceId);
        namedTemplate.getJdbcTemplate().update("create schema " + quote(instanceId.toString()));
    }


    @Override
    @WriteTransaction
    @SuppressWarnings("squid:S2077") // since instanceId must be a UUID, it is safe to use inline
    public void dropSchema(UUID instanceId) {
        namedTemplate.getJdbcTemplate().update("drop schema " + quote(instanceId.toString()) + " cascade");
        namedTemplate.getJdbcTemplate().update("delete from sys_wds.instance where id = ?", instanceId);
    }

}
