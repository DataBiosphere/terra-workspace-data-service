package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;

import bio.terra.common.db.WriteTransaction;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PostgresCollectionDao implements CollectionDao {

  private final NamedParameterJdbcTemplate namedTemplate;

  /*
  PostgresCollectionDao is used to interact with sys_wds instance table in postgres.
  NOTE: sys_wds.instance will be renamed to sys_wds.collection in an upcoming PR for AJ-1592
  This table tracks activity such as collection creation and deletion, as well as returning existing collections.
  This class will help add entries to the table, check if entries already exist and update them as necessary.
   */
  public PostgresCollectionDao(NamedParameterJdbcTemplate namedTemplate) {
    this.namedTemplate = namedTemplate;
  }

  @Override
  public boolean collectionSchemaExists(UUID collectionId) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from sys_wds.instance WHERE id = :collectionId)",
            new MapSqlParameterSource("collectionId", collectionId),
            Boolean.class));
  }

  @Override
  public List<UUID> listCollectionSchemas() {
    return namedTemplate
        .getJdbcTemplate()
        .queryForList("select id from sys_wds.instance order by id", UUID.class);
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since collectionId must be a UUID, it is safe to use inline
  public void createSchema(UUID collectionId) {
    namedTemplate
        .getJdbcTemplate()
        .update("insert into sys_wds.instance(id) values (?)", collectionId);
    namedTemplate.getJdbcTemplate().update("create schema " + quote(collectionId.toString()));
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since collectionId must be a UUID, it is safe to use inline
  public void dropSchema(UUID collectionId) {
    namedTemplate
        .getJdbcTemplate()
        .update("drop schema " + quote(collectionId.toString()) + " cascade");
    namedTemplate
        .getJdbcTemplate()
        .update("delete from sys_wds.instance where id = ?", collectionId);
  }

  @Override
  @WriteTransaction
  @SuppressWarnings("squid:S2077") // since collectionId must be a UUID, it is safe to use inline
  public void alterSchema(UUID oldSchemaId, UUID newSchemaId) {
    // rename the pg schema from old to new
    namedTemplate
        .getJdbcTemplate()
        .update(
            "alter schema "
                + quote(oldSchemaId.toString())
                + " rename to "
                + quote(newSchemaId.toString()));
    // rename any rows in sys_wds.instance from old to new
    namedTemplate
        .getJdbcTemplate()
        .update("update sys_wds.instance set id = ? where id = ?", newSchemaId, oldSchemaId);
    // ensure new exists in sys_wds.instance. When this alterSchema() method is called after
    // restoring from a pg_dump,
    // the oldSchema doesn't exist, so is not renamed in the previous statement.
    namedTemplate
        .getJdbcTemplate()
        .update(
            "insert into sys_wds.instance(id) values (?) on conflict(id) do nothing", newSchemaId);
  }
}
