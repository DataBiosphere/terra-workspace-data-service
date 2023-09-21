package org.databiosphere.workspacedataservice.dao;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class TestDao {

  @Autowired NamedParameterJdbcTemplate namedTemplate;

  @Autowired RecordDao recordDao;

  public boolean joinTableExists(
      UUID instanceId, String tableName, RecordType referringRecordType) {
    // This method gives us the name in quotes, need to be able to strip them off
    String joinTableName = recordDao.getJoinTableName(tableName, referringRecordType);
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from pg_tables where schemaname = :instanceId AND tablename  = :joinName)",
            new MapSqlParameterSource(
                Map.of(
                    "instanceId",
                    instanceId.toString(),
                    "joinName",
                    joinTableName.substring(1, joinTableName.length() - 1))),
            Boolean.class));
  }

  public List<String> getRelationArrayValues(
      UUID instanceId, String columnName, Record record, RecordType toRecordType) {
    return namedTemplate.queryForList(
        "select \""
            + recordDao.getToColumnName(toRecordType)
            + "\" from "
            + recordDao.getQualifiedJoinTableName(instanceId, columnName, record.getRecordType())
            + " where "
            + "\""
            + recordDao.getFromColumnName(record.getRecordType())
            + "\" = :recordId",
        new MapSqlParameterSource("recordId", record.getId()),
        String.class);
  }
}
