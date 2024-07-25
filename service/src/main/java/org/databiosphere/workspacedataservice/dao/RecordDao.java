package org.databiosphere.workspacedataservice.dao;

import static org.databiosphere.workspacedataservice.dao.SqlUtils.getQualifiedTableName;
import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;
import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;
import static org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException.NameType.ATTRIBUTE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedata.model.FilterColumn;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.service.model.RelationValue;
import org.databiosphere.workspacedataservice.service.model.exception.BatchDeleteException;
import org.databiosphere.workspacedataservice.service.model.exception.ConflictException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.SerializationException;
import org.databiosphere.workspacedataservice.shared.model.AttributeComparator;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordColumn;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.SearchFilter;
import org.databiosphere.workspacedataservice.shared.model.attributes.JsonAttribute;
import org.jetbrains.annotations.NotNull;
import org.postgresql.jdbc.PgArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;
import org.springframework.web.server.ResponseStatusException;

@Repository
public class RecordDao {

  private static final String COLLECTION_ID = "collectionId";
  private static final String RECORD_ID_PARAM = "recordId";
  private final NamedParameterJdbcTemplate namedTemplate;

  private final DataSource mainDb;

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordDao.class);

  private final DataTypeInferer inferer;

  private final ObjectMapper objectMapper;
  private final PrimaryKeyDao primaryKeyDao;

  @Value("${twds.streaming.fetch.size:5000}")
  int fetchSize;

  /**
   * Each member of this set is expected to be a set of two data types. The presence of a set of
   * types in this set implies that conversions between the types are supported (in either
   * direction). Note that arrays do not need to be listed separately here. For example, a set of
   * STRING and NUMBER implies that all the following conversions are supported: STRING to NUMBER,
   * NUMBER to STRING, ARRAY_OF_STRING to ARRAY_OF_NUMBER, and ARRAY_OF_NUMBER to ARRAY_OF_STRING.
   */
  private static final Set<Set<DataTypeMapping>> supportedDataTypeConversions =
      Set.of(
          Set.of(DataTypeMapping.STRING, DataTypeMapping.NUMBER),
          Set.of(DataTypeMapping.STRING, DataTypeMapping.BOOLEAN),
          Set.of(DataTypeMapping.STRING, DataTypeMapping.DATE),
          Set.of(DataTypeMapping.STRING, DataTypeMapping.DATE_TIME),
          Set.of(DataTypeMapping.NUMBER, DataTypeMapping.BOOLEAN),
          Set.of(DataTypeMapping.NUMBER, DataTypeMapping.DATE),
          Set.of(DataTypeMapping.NUMBER, DataTypeMapping.DATE_TIME),
          Set.of(DataTypeMapping.DATE, DataTypeMapping.DATE_TIME));

  /**
   * These error codes are expected when a valid update attribute data type request fails because
   * attribute values for some records cannot be converted to the new data type.
   *
   * @see <a href="https://www.postgresql.org/docs/14/errcodes-appendix.html">PostgreSQL error
   *     codes</a>
   */
  private static final Set<String> expectedDataTypeConversionErrorCodes =
      Set.of(
          "22P02", // invalid text representation
          "22003", // numeric value out of range
          "22008" // datetime field overflow
          );

  public RecordDao(
      DataSource mainDb,
      NamedParameterJdbcTemplate namedTemplate,
      DataTypeInferer inf,
      ObjectMapper objectMapper,
      PrimaryKeyDao primaryKeyDao) {
    this.mainDb = mainDb;
    this.namedTemplate = namedTemplate;
    this.inferer = inf;
    this.objectMapper = objectMapper;
    this.primaryKeyDao = primaryKeyDao;
  }

  public boolean recordTypeExists(UUID collectionId, RecordType recordType) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select from pg_tables where schemaname = :collectionId AND tablename  = :recordType)",
            new MapSqlParameterSource(
                Map.of(COLLECTION_ID, collectionId.toString(), "recordType", recordType.getName())),
            Boolean.class));
  }

  @SuppressWarnings("squid:S2077")
  public void createRecordType(
      UUID collectionId,
      Map<String, DataTypeMapping> tableInfo,
      RecordType recordType,
      RelationCollection relations,
      String recordTypePrimaryKey) {
    // this handles the case where the user incorrectly includes the primary key data in the
    // attributes
    tableInfo = Maps.filterKeys(tableInfo, k -> !k.equals(recordTypePrimaryKey));
    String columnDefs = genColumnDefs(tableInfo, recordTypePrimaryKey);
    try {
      namedTemplate
          .getJdbcTemplate()
          .update(
              "create table "
                  + getQualifiedTableName(recordType, collectionId)
                  + "( "
                  + columnDefs
                  + (!relations.relations().isEmpty()
                      ? ", " + getFkSql(relations.relations(), collectionId)
                      : "")
                  + ")");
      for (Relation relationArray : relations.relationArrays()) {
        createRelationJoinTable(
            collectionId,
            relationArray.relationColName(),
            recordType,
            relationArray.relationRecordType());
      }
    } catch (DataAccessException e) {
      if (e.getRootCause() instanceof SQLException sqlEx) {
        checkForMissingTable(sqlEx);
      }
      // this exception is thrown from getFkSql if the referenced relation doesn't exist
      if (e instanceof EmptyResultDataAccessException) {
        throw new MissingObjectException("Record type for relation");
      }
      throw e;
    }
  }

  public String getJoinTableName(String relationColumnName, RecordType fromTable) {
    // Use RESERVED_NAME_PREFIX to ensure no collision with user-named tables.
    // RecordType name has already been sql-validated
    return quote(
        RESERVED_NAME_PREFIX
            + fromTable.getName()
            + "_"
            + SqlUtils.validateSqlString(relationColumnName, ATTRIBUTE));
  }

  public String getQualifiedJoinTableName(
      UUID collectionId, String relationColumnName, RecordType fromTable) {
    return quote(collectionId.toString()) + "." + getJoinTableName(relationColumnName, fromTable);
  }

  public String getFromColumnName(RecordType referringRecordType) {
    return "from_" + referringRecordType.getName() + "_key";
  }

  public String getToColumnName(RecordType referencedRecordType) {
    return "to_" + referencedRecordType.getName() + "_key";
  }

  @SuppressWarnings("squid:S2077")
  public void createRelationJoinTable(
      UUID collectionId,
      String tableName,
      RecordType referringRecordType,
      RecordType referencedRecordType) {
    String fromCol = getFromColumnName(referringRecordType);
    String toCol = getToColumnName(referencedRecordType);
    String columnDefs = quote(fromCol) + " text, " + quote(toCol) + " text";
    try {
      namedTemplate
          .getJdbcTemplate()
          .update(
              "create table "
                  + getQualifiedJoinTableName(collectionId, tableName, referringRecordType)
                  + "( "
                  + columnDefs
                  + ", "
                  + getFkSqlForJoin(
                      new Relation(fromCol, referringRecordType),
                      new Relation(toCol, referencedRecordType),
                      collectionId)
                  + ")");
    } catch (DataAccessException e) {
      if (e.getRootCause() instanceof SQLException sqlEx) {
        checkForMissingTable(sqlEx);
      }
      // this exception is thrown from getFkSql if the referenced relation doesn't exist
      if (e instanceof EmptyResultDataAccessException) {
        throw new MissingObjectException("Record type for relation");
      }
      throw e;
    }
  }

  @SuppressWarnings("squid:S2077")
  public List<Record> queryForRecords(
      RecordType recordType,
      int pageSize,
      int offset,
      String sortDirection,
      @Nullable String sortAttribute, // this comes from SearchRequest, which might not be provided
      Optional<SearchFilter> searchFilter,
      UUID collectionId) {
    LOGGER.info("queryForRecords: {}", recordType.getName());

    // extract potential record ids from the `filter` param
    Optional<List<String>> filterIds = searchFilter.flatMap(SearchFilter::ids);

    // fail fast: filter.ids is provided, but no ids were specified.
    // Return an empty list of Records.
    // Should this be a Bad Request instead?
    if (filterIds.isPresent() && filterIds.get().isEmpty()) {
      return List.of();
    }

    // init a where clause (as the empty string) and bind params (as an empty map)
    StringBuilder whereClause = new StringBuilder();
    MapSqlParameterSource sqlParams = new MapSqlParameterSource();

    // if this query has specified filter.ids, populate the where clause and bind params
    if (filterIds.isPresent()) {
      // we know ids is non-empty due to the check above
      whereClause.append(" where ");
      whereClause.append(quote(primaryKeyDao.getPrimaryKeyColumn(recordType, collectionId)));
      whereClause.append(" in (:filterIds) ");
      sqlParams.addValue("filterIds", filterIds.get());
    }

    // if this query has specified filter.filters, populate the where clause and bind params
    Optional<List<FilterColumn>> filterColumns = searchFilter.flatMap(SearchFilter::filters);
    if (filterColumns.isPresent() && !filterColumns.get().isEmpty()) {
      if (whereClause.isEmpty()) {
        whereClause.append(" where 1=1");
      }

      var idx = 0;
      for (FilterColumn filter : filterColumns.get()) {
        // bind parameter names have syntax limitations, so we use an artificial one based on
        // the filter index
        var paramName = "filter" + idx;
        whereClause.append(" and ");
        whereClause.append(quote(filter.getColumn()));
        whereClause.append(" = :");
        whereClause.append(paramName);
        sqlParams.addValue(paramName, filter.getFind());
      }
    }

    return namedTemplate.query(
        "select * from "
            + getQualifiedTableName(recordType, collectionId)
            + whereClause
            + " order by "
            + (sortAttribute == null
                ? quote(primaryKeyDao.getPrimaryKeyColumn(recordType, collectionId))
                : quote(sortAttribute))
            + " "
            + sortDirection
            + " limit "
            + pageSize
            + " offset "
            + offset,
        sqlParams,
        new RecordRowMapper(recordType, objectMapper, collectionId));
  }

  public List<String> getAllAttributeNames(UUID collectionId, RecordType recordType) {
    MapSqlParameterSource params =
        new MapSqlParameterSource(COLLECTION_ID, collectionId.toString());
    params.addValue("tableName", recordType.getName());
    List<String> attributeNames =
        namedTemplate.queryForList(
            "select column_name from INFORMATION_SCHEMA.COLUMNS where table_schema = :collectionId "
                + "and table_name = :tableName",
            params,
            String.class);
    attributeNames.sort(
        new AttributeComparator(primaryKeyDao.getPrimaryKeyColumn(recordType, collectionId)));
    return attributeNames;
  }

  public Map<String, DataTypeMapping> getExistingTableSchema(
      UUID collectionId, RecordType recordType) {
    MapSqlParameterSource params =
        new MapSqlParameterSource(COLLECTION_ID, collectionId.toString());
    params.addValue("tableName", recordType.getName());
    String sql =
        "select column_name,coalesce(domain_name, udt_name::regtype::varchar) as data_type from INFORMATION_SCHEMA.COLUMNS "
            + "where table_schema = :collectionId and table_name = :tableName";
    return getTableSchema(sql, params);
  }

  private Map<String, DataTypeMapping> getTableSchema(String sql, MapSqlParameterSource params) {
    return namedTemplate.query(
        sql,
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

  public Map<String, DataTypeMapping> getExistingTableSchemaLessPrimaryKey(
      UUID collectionId, RecordType recordType) {
    MapSqlParameterSource params =
        new MapSqlParameterSource(COLLECTION_ID, collectionId.toString());
    params.addValue("tableName", recordType.getName());
    params.addValue("primaryKey", primaryKeyDao.getPrimaryKeyColumn(recordType, collectionId));
    String sql =
        "select column_name, coalesce(domain_name, udt_name::regtype::varchar) as data_type from INFORMATION_SCHEMA.COLUMNS where table_schema = :collectionId "
            + "and table_name = :tableName and column_name != :primaryKey";
    return getTableSchema(sql, params);
  }

  public void addColumn(
      UUID collectionId, RecordType recordType, String columnName, DataTypeMapping colType) {
    addColumn(collectionId, recordType, columnName, colType, null);
  }

  @SuppressWarnings("squid:S2077")
  public void addColumn(
      UUID collectionId,
      RecordType recordType,
      String columnName,
      DataTypeMapping colType,
      RecordType referencedType) {
    try {
      namedTemplate
          .getJdbcTemplate()
          .update(
              "alter table "
                  + getQualifiedTableName(recordType, collectionId)
                  + " add column "
                  + quote(SqlUtils.validateSqlString(columnName, ATTRIBUTE))
                  + " "
                  + colType.getPostgresType()
                  + (referencedType != null
                      ? " references " + getQualifiedTableName(referencedType, collectionId)
                      : ""));
    } catch (DataAccessException e) {
      if (e.getRootCause() instanceof SQLException sqlEx) {
        checkForMissingTable(sqlEx);
      }
      throw e;
    }
  }

  @SuppressWarnings("squid:S2077")
  public void changeColumn(
      UUID collectionId, RecordType recordType, String columnName, DataTypeMapping newColType) {
    namedTemplate
        .getJdbcTemplate()
        .update(
            "alter table "
                + getQualifiedTableName(recordType, collectionId)
                + " alter column "
                + quote(SqlUtils.validateSqlString(columnName, ATTRIBUTE))
                + " TYPE "
                + newColType.getPostgresType());
  }

  private String genColumnDefs(Map<String, DataTypeMapping> tableInfo, String primaryKeyCol) {
    return getPrimaryKeyDef(primaryKeyCol)
        + (tableInfo.size() > 0
            ? ", "
                + tableInfo.entrySet().stream()
                    .map(
                        e ->
                            quote(SqlUtils.validateSqlString(e.getKey(), ATTRIBUTE))
                                + " "
                                + e.getValue().getPostgresType())
                    .collect(Collectors.joining(", "))
            : "");
  }

  private String getPrimaryKeyDef(String primaryKeyCol) {
    return (primaryKeyCol.equals(RECORD_ID)
            ? quote(primaryKeyCol)
            : quote(SqlUtils.validateSqlString(primaryKeyCol, ATTRIBUTE)))
        + " text primary key";
  }

  // The expectation is that the record type already matches the schema and
  // attributes given, as
  // that's dealt with earlier in the code.
  public void batchUpsert(
      UUID collectionId,
      RecordType recordType,
      List<Record> records,
      Map<String, DataTypeMapping> schema,
      String primaryKeyColumn) {
    List<RecordColumn> schemaAsList = getSchemaWithRowId(schema, primaryKeyColumn);
    try {
      namedTemplate
          .getJdbcTemplate()
          .batchUpdate(
              genInsertStatement(collectionId, recordType, schemaAsList, primaryKeyColumn),
              getInsertBatchArgs(records, schemaAsList, primaryKeyColumn));
    } catch (DataAccessException e) {
      if (e.getRootCause() instanceof SQLException sqlEx) {
        checkForMissingRecord(sqlEx);
      }
      throw e;
    }
  }

  public void insertIntoJoin(
      UUID collectionId, Relation column, RecordType recordType, List<RelationValue> relations) {
    try {
      namedTemplate
          .getJdbcTemplate()
          .batchUpdate(
              genJoinInsertStatement(collectionId, column, recordType),
              getJoinInsertBatchArgs(relations));
    } catch (DataAccessException e) {
      if (e.getRootCause() instanceof SQLException sqlEx) {
        checkForMissingRecord(sqlEx);
      }
      throw e;
    }
  }

  public void removeFromJoin(
      UUID collectionId, Relation column, RecordType fromType, List<String> recordIds) {
    namedTemplate.update(
        "delete from "
            + getQualifiedJoinTableName(collectionId, column.relationColName(), fromType)
            + " where "
            + quote(getFromColumnName(fromType))
            + "in (:recordIds)",
        new MapSqlParameterSource("recordIds", recordIds));
  }

  public void batchUpsert(
      UUID collectionId,
      RecordType recordType,
      List<Record> records,
      Map<String, DataTypeMapping> schema) {
    batchUpsert(
        collectionId,
        recordType,
        records,
        schema,
        primaryKeyDao.getPrimaryKeyColumn(recordType, collectionId));
  }

  private List<RecordColumn> getSchemaWithRowId(
      Map<String, DataTypeMapping> schema, String recordIdColumnName) {
    // Make sure the id column is included, and that it is a string.
    // This will overwrite any other datatype the id column may have been set to.
    // WDS requires the id column to be a string (see getPrimaryKeyDef), so we need to enforce that.
    HashMap<String, DataTypeMapping> workingSchema = new HashMap<>(schema);
    workingSchema.put(recordIdColumnName, DataTypeMapping.STRING);
    return workingSchema.entrySet().stream()
        .map(e -> new RecordColumn(e.getKey(), e.getValue()))
        .toList();
  }

  public boolean deleteSingleRecord(UUID collectionId, RecordType recordType, String recordId) {
    String recordTypePrimaryKey = primaryKeyDao.getPrimaryKeyColumn(recordType, collectionId);
    try {
      return namedTemplate.update(
              "delete from "
                  + getQualifiedTableName(recordType, collectionId)
                  + " where "
                  + quote(recordTypePrimaryKey)
                  + " = :recordId",
              new MapSqlParameterSource(RECORD_ID_PARAM, recordId))
          == 1;
    } catch (DataIntegrityViolationException e) {
      if (e.getRootCause() instanceof SQLException sqlEx) {
        checkForTableRelation(sqlEx);
      }
      throw e;
    }
  }

  public void addForeignKeyForReference(
      RecordType recordType,
      RecordType referencedRecordType,
      UUID collectionId,
      String relationColName) {
    try {
      String addFk =
          "alter table "
              + getQualifiedTableName(recordType, collectionId)
              + " add foreign key ("
              + quote(relationColName)
              + ") "
              + "references "
              + getQualifiedTableName(referencedRecordType, collectionId);
      namedTemplate.getJdbcTemplate().execute(addFk);
    } catch (DataAccessException e) {
      if (e.getRootCause() instanceof SQLException sqlEx) {
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
        throw new ResponseStatusException(
            HttpStatus.BAD_REQUEST,
            "Unable to delete this record because another record has a relation to it.");
      }
      if (sqlEx.getSQLState().equals("2BP01")) {
        throw new ResponseStatusException(
            HttpStatus.CONFLICT,
            "Unable to delete this record type because another record type has a relation to it.");
      }
    }
  }

  @SuppressWarnings("squid:S2077") // sql statement has been manually reviewed
  public Stream<Record> streamAllRecordsForType(UUID collectionId, RecordType recordType) {
    // create the SQL for the query
    String sql =
        "select * from "
            + getQualifiedTableName(recordType, collectionId)
            + " order by "
            + quote(primaryKeyDao.getPrimaryKeyColumn(recordType, collectionId));

    // create the RowMapper, to translate JDBC rows to Record objects
    RecordRowMapper rrm = new RecordRowMapper(recordType, objectMapper, collectionId);

    // Spring Batch convenience to get a db connection, set autocommit=false on that connection,
    // prepare a SQL statement and set the fetch size on that statement, set a RowMapper,
    // and return all of this encapsulated in a Spring Batch ItemReader.
    // the ItemReader will manage the underlying Postgres db cursor and ultimately
    // make multiple queries to the db, returning `fetchSize` rows each time.
    // Hikari will reset the autocommit value on this connection when returning it to the pool.
    //
    // Per https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor,
    // requirements for streaming are:
    // 		- the connection must have autocommit=off
    //		- the statement must use forward-only fetching (which is the default)
    //		- the statement must have a positive fetch size
    JdbcCursorItemReader<Record> itemReader =
        new JdbcCursorItemReaderBuilder<Record>()
            .dataSource(mainDb)
            .connectionAutoCommit(false)
            .fetchSize(fetchSize)
            .sql(sql)
            .rowMapper(rrm)
            .name(collectionId + "_" + recordType.getName()) // name is required but not important
            .build();

    // open the ItemReader
    itemReader.open(new ExecutionContext());

    // Spliterator implementation that wraps the ItemReader:
    // in essence, each call to Spliterator.tryAdvance() maps
    // to a call to ItemReader.read().
    // This Spliterator is the necessary stepping stone between ItemReader and Stream.
    Spliterators.AbstractSpliterator<Record> spliterator =
        new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE, Spliterator.ORDERED) {
          @Override
          public boolean tryAdvance(Consumer<? super Record> action) {
            try {
              Record item = itemReader.read();
              if (item == null) {
                return false;
              }
              action.accept(item);
              return true;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };

    // map the ItemReader to a Stream, via StreamSupport
    // ensure the ItemReader closes when the stream closes
    return StreamSupport.stream(spliterator, false).onClose(itemReader::close);
  }

  public String getFkSql(Set<Relation> relations, UUID collectionId) {
    return relations.stream()
        .map(r -> getFkSql(r, collectionId, false))
        .collect(Collectors.joining(", \n"));
  }

  public String getFkSql(Relation r, UUID collectionId, boolean cascade) {
    return "constraint "
        + quote("fk_" + SqlUtils.validateSqlString(r.relationColName(), ATTRIBUTE))
        + " foreign key ("
        + quote(SqlUtils.validateSqlString(r.relationColName(), ATTRIBUTE))
        + ") references "
        + getQualifiedTableName(r.relationRecordType(), collectionId)
        + "("
        + quote(primaryKeyDao.getPrimaryKeyColumn(r.relationRecordType(), collectionId))
        + ")"
        + (cascade ? " on delete cascade" : "");
  }

  public String getFkSqlForJoin(Relation fromRelation, Relation toRelation, UUID collectionId) {
    return getFkSql(fromRelation, collectionId, true)
        + ", \n"
        + getFkSql(toRelation, collectionId, false);
  }

  public List<Relation> getRelationCols(UUID collectionId, RecordType recordType) {
    return namedTemplate.query(
        "SELECT kcu.column_name, ccu.table_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu "
            + "ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
            + "JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema "
            + "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = :workspace AND tc.table_name= :tableName",
        Map.of("workspace", collectionId.toString(), "tableName", recordType.getName()),
        (rs, rowNum) ->
            new Relation(
                rs.getString("column_name"), RecordType.valueOf(rs.getString("table_name"))));
  }

  public List<Relation> getRelationArrayCols(UUID collectionId, RecordType recordType) {
    return namedTemplate.query(
        "select kcu1.table_name, kcu1.column_name from information_schema.key_column_usage kcu1 join information_schema.key_column_usage kcu2 "
            + "on kcu1.table_name = kcu2.table_name where kcu1.constraint_schema = :workspace and kcu2.constraint_name = :from_table_constraint"
            + " and kcu2.constraint_name != kcu1.constraint_name",
        Map.of(
            "workspace",
            collectionId.toString(),
            "from_table_constraint",
            "fk_from_" + recordType.getName() + "_key"),
        new RelationRowMapper(recordType));
  }

  private static class RelationRowMapper implements RowMapper<Relation> {

    private final RecordType recordType;

    public RelationRowMapper(RecordType recordType) {
      this.recordType = recordType;
    }

    @Override
    public Relation mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new Relation(
          getAttributeFromTableName(rs.getString("table_name")),
          getRecordTypeFromConstraint(rs.getString("column_name")));
    }

    private RecordType getRecordTypeFromConstraint(String constraint) {
      // constraint should be to_tablename_key
      return RecordType.valueOf(
          StringUtils.removeEnd(StringUtils.removeStart(constraint, "to_"), "_key"));
    }

    private String getAttributeFromTableName(String tableName) {
      // table will be RESERVED_NAME_PREFIX_fromTable_attribute
      return StringUtils.removeStart(
          tableName, RESERVED_NAME_PREFIX + this.recordType.getName() + "_");
    }
  }

  @SuppressWarnings("squid:S2077")
  public int countRecords(UUID collectionId, RecordType recordType) {
    return namedTemplate
        .getJdbcTemplate()
        .queryForObject(
            "select count(*) from " + getQualifiedTableName(recordType, collectionId),
            Integer.class);
  }

  private String genColUpsertUpdates(List<String> cols, String recordTypeRowIdentifier) {
    return cols.stream()
        .filter(c -> !recordTypeRowIdentifier.equals(c))
        .map(c -> quote(c) + " = excluded." + quote(c))
        .collect(Collectors.joining(", "));
  }

  private List<Object[]> getInsertBatchArgs(
      List<Record> records, List<RecordColumn> cols, String recordTypeRowIdentifier) {
    return records.stream().map(r -> getInsertArgs(r, cols, recordTypeRowIdentifier)).toList();
  }

  private Object getValueForSql(Object attVal, DataTypeMapping typeMapping) {
    if (Objects.isNull(attVal)) {
      return null;
    }
    if (RelationUtils.isRelationValue(attVal) && typeMapping == DataTypeMapping.RELATION) {
      return RelationUtils.getRelationValue(attVal);
    }
    if (attVal instanceof String sVal) {
      if (stringIsCompatibleWithType(
          typeMapping == DataTypeMapping.NUMBER, inferer::isNumericValue, sVal)) {
        return new BigDecimal(sVal);
      }
      if (stringIsCompatibleWithType(
          typeMapping == DataTypeMapping.BOOLEAN, inferer::isValidBoolean, sVal)) {
        return Boolean.parseBoolean(sVal);
      }
      if (stringIsCompatibleWithType(
          typeMapping == DataTypeMapping.DATE, inferer::isValidDate, sVal)) {
        return LocalDate.parse(sVal, DateTimeFormatter.ISO_LOCAL_DATE);
      }
      if (stringIsCompatibleWithType(
          typeMapping == DataTypeMapping.DATE_TIME, inferer::isValidDateTime, sVal)) {
        return LocalDateTime.parse(sVal, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
      }
    }
    // TSV-based uploads deserialize json as JsonAttribute.
    if (attVal instanceof JsonAttribute jsonAttribute) {
      try {
        return objectMapper.writeValueAsString(jsonAttribute.sqlValue());
      } catch (JsonProcessingException e) {
        throw new SerializationException("Could not serialize JsonAttribute to json string", e);
      }
    }
    // json-based APIs deserialize json as LinkedHashMap<String, Object>. Handle those here.
    // TODO AJ-1748: how to deserialize json-based APIs into JsonAttribute instead?
    if (attVal instanceof Map<?, ?>) {
      try {
        return objectMapper.writeValueAsString(attVal);
      } catch (JsonProcessingException e) {
        throw new SerializationException("Could not serialize Map to json string", e);
      }
    }
    if (typeMapping.isArrayType()) {
      return getArrayValues(attVal, typeMapping);
    }
    if (attVal instanceof List<?> list && typeMapping == DataTypeMapping.STRING) {
      return "{" + list.stream().map(Object::toString).collect(Collectors.joining(",")) + "}";
    }
    return attVal;
  }

  private Object getArrayValues(Object attVal, DataTypeMapping typeMapping) {
    if (attVal instanceof List<?> valAsList) {
      return getListAsArray(valAsList, typeMapping);
    }
    return inferer.getArrayOfType(attVal.toString(), typeMapping.getJavaArrayTypeForDbWrites());
  }

  @VisibleForTesting
  Object[] getListAsArray(List<?> attVal, DataTypeMapping typeMapping) {
    return switch (typeMapping) {
      case ARRAY_OF_STRING,
          ARRAY_OF_FILE,
          ARRAY_OF_RELATION,
          ARRAY_OF_DATE,
          ARRAY_OF_DATE_TIME,
          ARRAY_OF_NUMBER,
          EMPTY_ARRAY -> attVal.stream()
          .map(e -> Objects.toString(e, null)) // .toString() non-nulls, else return null
          .toList()
          .toArray(new String[0]);
      case ARRAY_OF_BOOLEAN ->
      // accept all casings of True and False if they're strings
      attVal.stream()
          .map(Object::toString)
          .map(String::toLowerCase)
          .map(Boolean::parseBoolean)
          .toList()
          .toArray(new Boolean[0]);
      case ARRAY_OF_JSON -> attVal.stream()
          .map(
              el -> {
                try {
                  return objectMapper.writeValueAsString(el);
                } catch (JsonProcessingException e) {
                  throw new SerializationException(
                      "Could not serialize array element to json string", e);
                }
              })
          .toList()
          .toArray(new String[0]);
      default -> throw new IllegalArgumentException("Unhandled array type " + typeMapping);
    };
  }

  private boolean stringIsCompatibleWithType(
      boolean typesMatch, Predicate<String> typeCheckPredicate, String attVal) {
    return typesMatch && typeCheckPredicate.test(attVal);
  }

  private Object[] getInsertArgs(
      Record toInsert, List<RecordColumn> cols, String recordTypeRowIdentifier) {
    Object[] row = new Object[cols.size()];
    int i = 0;
    for (RecordColumn col : cols) {
      String colName = col.colName();
      if (colName.equals(recordTypeRowIdentifier)) {
        row[i++] = toInsert.getId();
      } else {
        row[i++] = getValueForSql(toInsert.getAttributeValue(colName), col.typeMapping());
      }
    }
    return row;
  }

  private List<Object[]> getJoinInsertBatchArgs(List<RelationValue> relations) {
    return relations.stream()
        .map(r -> new Object[] {r.fromRecord().getId(), r.toRecord().getId()})
        .toList();
  }

  private String genInsertStatement(
      UUID collectionId,
      RecordType recordType,
      List<RecordColumn> schema,
      String recordTypeIdenifier) {
    List<String> colNames = schema.stream().map(RecordColumn::colName).toList();
    List<DataTypeMapping> colTypes = schema.stream().map(RecordColumn::typeMapping).toList();
    return "insert into "
        + getQualifiedTableName(recordType, collectionId)
        + "("
        + getInsertColList(colNames)
        + ") values ("
        + getInsertParamList(colTypes)
        + ") "
        + "on conflict ("
        + quote(recordTypeIdenifier)
        + ") "
        + (schema.size() == 1
            ? "do nothing"
            : "do update set " + genColUpsertUpdates(colNames, recordTypeIdenifier));
  }

  private String genJoinInsertStatement(
      UUID collectionId, Relation relation, RecordType recordType) {
    String fromCol = getFromColumnName(recordType);
    String toCol = getToColumnName(relation.relationRecordType());
    String columnDefs = " (" + quote(fromCol) + ", " + quote(toCol) + ")";
    return "insert into "
        + getQualifiedJoinTableName(collectionId, relation.relationColName(), recordType)
        + columnDefs
        + " values (?,?) ";
  }

  private String getInsertParamList(List<DataTypeMapping> colTypes) {
    return colTypes.stream()
        .map(DataTypeMapping::getWritePlaceholder)
        .collect(Collectors.joining(", "));
  }

  private String getInsertColList(List<String> existingTableSchema) {
    return existingTableSchema.stream().map(SqlUtils::quote).collect(Collectors.joining(", "));
  }

  @SuppressWarnings("squid:S2077")
  public void batchDelete(UUID collectionId, RecordType recordType, List<Record> records) {
    List<String> recordIds = records.stream().map(Record::getId).toList();
    try {
      int[] rowCounts =
          namedTemplate
              .getJdbcTemplate()
              .batchUpdate(
                  "delete from"
                      + getQualifiedTableName(recordType, collectionId)
                      + " where "
                      + quote(primaryKeyDao.getPrimaryKeyColumn(recordType, collectionId))
                      + " = ?",
                  new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(@NotNull PreparedStatement ps, int i)
                        throws SQLException {
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
          recordErrors.add(
              "record id " + recordIds.get(i) + " does not exist in " + recordType.getName());
        }
      }
      if (!recordErrors.isEmpty()) {
        throw new BatchDeleteException(recordErrors);
      }
    } catch (DataIntegrityViolationException e) {
      if (e.getRootCause() instanceof SQLException sqlEx) {
        checkForTableRelation(sqlEx);
      }
      throw e;
    }
  }

  private class RecordRowMapper implements RowMapper<Record> {

    private final RecordType recordType;

    private final Map<String, RecordType> referenceColToTable;

    private final ObjectMapper objectMapper;

    private final Map<String, DataTypeMapping> schema;

    private final String primaryKeyColumn;

    public RecordRowMapper(RecordType recordType, ObjectMapper objectMapper, UUID collectionId) {
      this.recordType = recordType;
      this.objectMapper = objectMapper;
      this.schema = RecordDao.this.getExistingTableSchemaLessPrimaryKey(collectionId, recordType);
      this.primaryKeyColumn = primaryKeyDao.getPrimaryKeyColumn(recordType, collectionId);
      this.referenceColToTable =
          RecordDao.this.getRelationColumnsByName(
              RecordDao.this.getRelationCols(collectionId, recordType));
    }

    @Override
    public Record mapRow(ResultSet rs, int rowNum) throws SQLException {
      ResultSetMetaData metaData = rs.getMetaData();

      // ResultSet's getter methods (getString, etc.) do not respect case of column names.
      // If multiple columns have the same name differing only in case (for example, "attr" vs
      // "Attr"),
      // then getter methods will return the value of the first matching column.
      // Because of this, we must get values by column index instead of name.
      int primaryKeyColumnIndex = -1;
      for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
        String columnName = metaData.getColumnName(columnIndex);
        if (columnName.equals(primaryKeyColumn)) {
          primaryKeyColumnIndex = columnIndex;
          break;
        }
      }
      if (primaryKeyColumnIndex == -1) {
        throw new RuntimeException(
            "Primary key column \"%s\" not found".formatted(primaryKeyColumn));
      }

      try {
        return new Record(rs.getString(primaryKeyColumnIndex), recordType, getAttributes(rs));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    private RecordAttributes getAttributes(ResultSet rs) throws JsonProcessingException {
      try {
        ResultSetMetaData metaData = rs.getMetaData();
        RecordAttributes attributes = RecordAttributes.empty(primaryKeyColumn);

        // ResultSet's getter methods (getString, etc.) do not respect case of column names.
        // If multiple columns have the same name differing only in case (for example, "attr" vs
        // "Attr"),
        // then getter methods will return the value of the first matching column.
        // Because of this, we must get values by column index instead of name.
        for (int columnIndex = 1; columnIndex <= metaData.getColumnCount(); columnIndex++) {
          String columnName = metaData.getColumnName(columnIndex);
          if (columnName.equals(primaryKeyColumn)) {
            attributes.putAttribute(primaryKeyColumn, rs.getString(columnIndex));
            continue;
          }
          if (referenceColToTable.size() > 0
              && referenceColToTable.containsKey(columnName)
              && rs.getString(columnName) != null) {
            attributes.putAttribute(
                columnName,
                RelationUtils.createRelationString(
                    referenceColToTable.get(columnName), rs.getString(columnIndex)));
          } else {
            attributes.putAttribute(
                columnName,
                getAttributeValueForType(rs.getObject(columnIndex), schema.get(columnName)));
          }
        }
        return attributes;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    private Object getAttributeValueForType(Object object, DataTypeMapping typeMapping)
        throws SQLException, JsonProcessingException {
      if (object == null) {
        return null;
      }
      if (object instanceof java.sql.Date date && typeMapping == DataTypeMapping.DATE) {
        return date.toLocalDate();
      }
      if (object instanceof java.sql.Timestamp ts && typeMapping == DataTypeMapping.DATE_TIME) {
        return ts.toLocalDateTime();
      }
      if (typeMapping.isArrayType() && object instanceof PgArray pgArray) {
        return getArrayValue(pgArray.getArray(), typeMapping);
      }
      if (typeMapping == DataTypeMapping.JSON) {
        return new JsonAttribute(objectMapper.readTree(object.toString()));
      }
      return object;
    }

    private Object getArrayValue(Object object, DataTypeMapping typeMapping)
        throws JsonProcessingException {
      if (typeMapping == DataTypeMapping.ARRAY_OF_DATE_TIME) {
        return convertToLocalDateTime(object);
      } else if (typeMapping == DataTypeMapping.ARRAY_OF_DATE) {
        return convertToLocalDate(object);
      } else if (typeMapping == DataTypeMapping.ARRAY_OF_JSON) {
        return convertToJson(object);
      }
      return object;
    }

    private LocalDateTime[] convertToLocalDateTime(Object object) {
      Timestamp[] tzArray = (Timestamp[]) object;
      LocalDateTime[] result = new LocalDateTime[tzArray.length];
      for (int i = 0; i < tzArray.length; i++) {
        result[i] = tzArray[i].toLocalDateTime();
      }
      return result;
    }

    private LocalDate[] convertToLocalDate(Object object) {
      Date[] tzArray = (Date[]) object;
      LocalDate[] result = new LocalDate[tzArray.length];
      for (int i = 0; i < tzArray.length; i++) {
        result[i] = tzArray[i].toLocalDate();
      }
      return result;
    }

    private Object[] convertToJson(Object object) throws JsonProcessingException {
      // json arrays are returned from the db as String[]
      String[] jsonArray = (String[]) object;
      JsonAttribute[] result = new JsonAttribute[jsonArray.length];
      for (int i = 0; i < jsonArray.length; i++) {
        result[i] = new JsonAttribute(objectMapper.readTree(jsonArray[i]));
      }
      return result;
    }
  }

  public Optional<Record> getSingleRecord(
      UUID collectionId, RecordType recordType, String recordId) {
    try {
      return Optional.ofNullable(
          namedTemplate.queryForObject(
              "select * from "
                  + getQualifiedTableName(recordType, collectionId)
                  + " where "
                  + quote(primaryKeyDao.getPrimaryKeyColumn(recordType, collectionId))
                  + " = :recordId",
              new MapSqlParameterSource(RECORD_ID_PARAM, recordId),
              new RecordRowMapper(recordType, objectMapper, collectionId)));
    } catch (EmptyResultDataAccessException e) {
      return Optional.empty();
    }
  }

  public String getPrimaryKeyColumn(RecordType type, UUID collectionId) {
    return primaryKeyDao.getPrimaryKeyColumn(type, collectionId);
  }

  public boolean recordExists(UUID collectionId, RecordType recordType, String recordId) {
    return Boolean.TRUE.equals(
        namedTemplate.queryForObject(
            "select exists(select * from "
                + getQualifiedTableName(recordType, collectionId)
                + " where "
                + quote(primaryKeyDao.getPrimaryKeyColumn(recordType, collectionId))
                + " = :recordId)",
            new MapSqlParameterSource(RECORD_ID_PARAM, recordId),
            Boolean.class));
  }

  public List<RecordType> getAllRecordTypes(UUID collectionId) {
    return namedTemplate.queryForList(
        "select tablename from pg_tables WHERE schemaname = :workspaceSchema and tablename not like 'sys_%' order by tablename",
        new MapSqlParameterSource("workspaceSchema", collectionId.toString()), RecordType.class);
  }

  Map<String, RecordType> getRelationColumnsByName(List<Relation> referenceCols) {
    return referenceCols.stream()
        .collect(Collectors.toMap(Relation::relationColName, Relation::relationRecordType));
  }

  // AJ-1242: if/when we re-enable caching for primary key values, this method should gain a
  // @CacheEvict annotation
  public void deleteRecordType(UUID collectionId, RecordType recordType) {
    List<Relation> relationArrayCols = getRelationArrayCols(collectionId, recordType);
    for (Relation rel : relationArrayCols) {
      namedTemplate
          .getJdbcTemplate()
          .update(
              "drop table "
                  + getQualifiedJoinTableName(collectionId, rel.relationColName(), recordType));
    }
    // TODO: AJ-1624 This line generates a `java:S2077` warning in sonar.  That warning was
    // suppressed in the
    // sonar UI but could be moved to a line-level suppression in the code
    try {
      namedTemplate
          .getJdbcTemplate()
          .update("drop table " + getQualifiedTableName(recordType, collectionId));
    } catch (DataAccessException e) {
      if (e.getRootCause() instanceof SQLException sqlEx) {
        checkForTableRelation(sqlEx);
      }
      throw e;
    }
  }

  public void renameAttribute(
      UUID collectionId, RecordType recordType, String attribute, String newAttributeName) {
    namedTemplate
        .getJdbcTemplate()
        .update(
            "alter table "
                + getQualifiedTableName(recordType, collectionId)
                + " rename column "
                + quote(SqlUtils.validateSqlString(attribute, ATTRIBUTE))
                + " to "
                + quote(SqlUtils.validateSqlString(newAttributeName, ATTRIBUTE)));
  }

  public void updateAttributeDataType(
      UUID collectionId, RecordType recordType, String attribute, DataTypeMapping newDataType) {
    Map<String, DataTypeMapping> schema = getExistingTableSchema(collectionId, recordType);
    DataTypeMapping currentDataType = schema.get(attribute);

    try {
      namedTemplate
          .getJdbcTemplate()
          .update(
              "alter table "
                  + getQualifiedTableName(recordType, collectionId)
                  + " alter column "
                  + quote(SqlUtils.validateSqlString(attribute, ATTRIBUTE))
                  + " type "
                  + newDataType.getPostgresType()
                  + " using "
                  + getPostgresTypeConversionExpression(attribute, currentDataType, newDataType));
    } catch (DataIntegrityViolationException e) {
      if (e.getRootCause() instanceof SQLException sqlEx && sqlEx.getSQLState() != null) {
        if (expectedDataTypeConversionErrorCodes.contains(sqlEx.getSQLState())) {
          throw new ConflictException(
              "Unable to convert values for attribute %s to %s"
                  .formatted(attribute, newDataType.name()));
        } else {
          LOGGER.warn(
              "updateAttributeDataType: DataIntegrityViolationException with unexpected error code {}",
              sqlEx.getSQLState());
        }
      }
      throw e;
    }
  }

  private boolean isDataTypeConversionSupported(
      DataTypeMapping dataType, DataTypeMapping newDataType) {
    DataTypeMapping baseType = dataType.getBaseType();
    DataTypeMapping newBaseType = newDataType.getBaseType();
    return baseType.equals(newBaseType)
        || supportedDataTypeConversions.contains(Set.of(baseType, newBaseType));
  }

  @VisibleForTesting
  String getPostgresTypeConversionExpression(
      String attribute, DataTypeMapping dataType, DataTypeMapping newDataType) {
    // Some data types are not yet supported.
    // Some conversions don't make sense / are invalid.
    if (!isDataTypeConversionSupported(dataType, newDataType)) {
      throw new IllegalArgumentException(
          "Unable to convert attribute from %s to %s"
              .formatted(dataType.name(), newDataType.name()));
    }

    // Prevent conversion from array to scalar types.
    if (dataType.isArrayType() && !newDataType.isArrayType()) {
      throw new IllegalArgumentException("Unable to convert array type to scalar type");
    }

    String expression = quote(SqlUtils.validateSqlString(attribute, ATTRIBUTE));

    // Unable to cast numbers to dates or timestamps.
    // Convert number to timestamp using to_timestamp.
    if (dataType.getBaseType().equals(DataTypeMapping.NUMBER)
        && Set.of(DataTypeMapping.DATE, DataTypeMapping.DATE_TIME)
            .contains(newDataType.getBaseType())) {
      if (dataType.isArrayType()) {
        expression = "(sys_wds.convert_array_of_numbers_to_timestamps(" + expression + "))";
      } else {
        expression = "to_timestamp(" + expression + ")";
      }
    }

    // Unable to cast dates or timestamps to numbers.
    // Extract epoch from timestamp to get a number.
    if (Set.of(DataTypeMapping.DATE, DataTypeMapping.DATE_TIME).contains(dataType.getBaseType())
        && newDataType.getBaseType().equals(DataTypeMapping.NUMBER)) {
      if (dataType.isArrayType()) {
        expression = "sys_wds.convert_array_of_timestamps_to_numbers(" + expression + ")";
        // Truncate to an integer for dates.
        if (dataType.getBaseType() == DataTypeMapping.DATE) {
          expression += "::bigint[]";
        }
        expression = "(" + expression + ")";
      } else {
        expression = "extract(epoch from " + expression + ")";
        // Truncate to an integer for dates.
        if (dataType.getBaseType() == DataTypeMapping.DATE) {
          expression += "::bigint";
        }
      }
    }

    // When converting from a scalar type to an array type, append value to an empty array.
    if (!dataType.isArrayType() && newDataType.isArrayType()) {
      expression = "array_append('{}', " + expression + ")";
    }

    // No direct conversion exists between numeric and boolean types, so go through int.
    if (Set.copyOf(List.of(dataType.getBaseType(), newDataType.getBaseType()))
        .equals(Set.of(DataTypeMapping.BOOLEAN, DataTypeMapping.NUMBER))) {
      expression += "::int";
      if (newDataType.isArrayType()) {
        expression += "[]";
      }
    }

    // Convert to desired type.
    expression += "::" + newDataType.getPostgresType();

    return expression;
  }

  public void deleteAttribute(UUID collectionId, RecordType recordType, String attribute) {
    namedTemplate
        .getJdbcTemplate()
        .update(
            "alter table "
                + getQualifiedTableName(recordType, collectionId)
                + " drop column "
                + quote(SqlUtils.validateSqlString(attribute, ATTRIBUTE)));
  }
}
