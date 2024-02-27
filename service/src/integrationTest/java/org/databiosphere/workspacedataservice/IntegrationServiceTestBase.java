package org.databiosphere.workspacedataservice;

import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class IntegrationServiceTestBase {

  /**
   * Reusable utility to wipe the db of all user state created by integration tests, while leaving
   * in place the sys_wds.* table definitions (i.e. don't need to re-run Liquibase)
   *
   * @param collectionDao dao to use for collection management
   * @param namedTemplate to use for direct SQL queries
   */
  protected void cleanDb(CollectionDao collectionDao, NamedParameterJdbcTemplate namedTemplate) {
    // drop all rows from backup, clone, restore tables
    // may need to add the job table at some point; current integration tests do not write to it
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.backup");
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.clone");
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.restore");

    // delete all collections that are currently known to WDS
    collectionDao.listCollectionSchemas().forEach(collectionDao::dropSchema);

    // and drop any orphaned Postgres schemas
    dropOrphanedSchemas(namedTemplate);
  }

  /**
   * Clean the db of any orphaned Postgres schemas - schemas that were created by WDS as a
   * collection but which do not have a corresponding row in sys_wds.collection. This can happen
   * when restores fail.
   *
   * <p>This is a standalone method in order to @SuppressWarnings on it in a targeted fashion. This
   * method uses string concatenation to build a SQL statement, since we cannot use bind params in a
   * `drop schema` statement. Since the value we are concatenating already validated as a UUID, we
   * know it does not contain SQL injection/escape sequences.
   *
   * @param namedTemplate to use for direct SQL queries
   */
  @SuppressWarnings("squid:S2077")
  private void dropOrphanedSchemas(NamedParameterJdbcTemplate namedTemplate) {
    namedTemplate
        .queryForList(
            "SELECT schema_name FROM information_schema.schemata;", Map.of(), String.class)
        .forEach(
            schemaName -> {
              // is this a UUID? We only want to drop orphaned schemas whose name is a UUID;
              // we don't want to drop other critical schemas like "public" or "sys_wds"
              try {
                UUID schemaUuid = UUID.fromString(schemaName);
                namedTemplate.update("DROP schema \"" + schemaUuid + "\" cascade;", Map.of());
              } catch (IllegalArgumentException iae) {
                // schema name was not a valid uuid; we should not drop this schema.
              }
            });
  }
}
