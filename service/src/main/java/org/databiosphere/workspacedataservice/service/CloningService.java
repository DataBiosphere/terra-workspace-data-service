package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;

import bio.terra.common.db.WriteTransaction;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.IteratorUtils;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.service.model.exception.CloningException;
import org.databiosphere.workspacedataservice.shared.model.DefaultCollectionCreationResult;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class CloningService {

  private final CollectionService collectionService;
  private final NamedParameterJdbcTemplate namedTemplate;
  private final RecordDao recordDao;

  private static final Logger logger = LoggerFactory.getLogger(CloningService.class);

  public CloningService(
      CollectionService collectionService,
      NamedParameterJdbcTemplate namedTemplate,
      RecordDao recordDao) {
    this.collectionService = collectionService;
    this.namedTemplate = namedTemplate;
    this.recordDao = recordDao;
  }

  @WriteTransaction
  public DefaultCollectionCreationResult clone(
      WorkspaceId sourceWorkspaceId, WorkspaceId targetWorkspaceId) {

    /*
     pseudocode:
         1. for each collection in the source,
         2. create a corresponding collection the target
         3. build a dependency graph of the collection's tables and their foreign keys.
         4. for each table, in dependency order, create the table schemas.
         5. for each table, copy data.
    */

    // check if the target workspace already contains any collections
    if (!collectionService.list(targetWorkspaceId).isEmpty()) {
      // TODO AJ-1952: find a better exception to throw; this should be a 400 bad request, not a 500
      //   internal server error
      throw new CloningException("Cannot clone into this workspace; it is not empty.");
    }

    // TODO AJ-1952: turn down logging throughout; it's pretty verbose for now
    logger.info(
        "Starting clone from source workspace {} to target workspace {}",
        sourceWorkspaceId,
        targetWorkspaceId);

    // create the collections in the target workspace and calculate the source->target id mapping
    Map<UUID, UUID> collectionMapping =
        copyCollectionDefinitions(sourceWorkspaceId, targetWorkspaceId);

    // for each source collection in the source workspace
    collectionMapping.forEach(
        (sourceCollectionId, targetCollectionId) -> {
          logger.info("... processing source collection {}", sourceCollectionId);

          // build the dependency graph of tables in this collection, so we know what order to copy
          // them
          List<String> tableOrder = getTableOrder(sourceCollectionId);

          // create the table schemas for each table. For this step, we skip the "sys_" join tables;
          // those will
          // be created automatically when we create the tables they support.
          logger.info("... creating table schemas for target collection {}", targetCollectionId);
          tableOrder.stream()
              .filter(tableName -> !tableName.startsWith(RESERVED_NAME_PREFIX))
              .forEach(
                  tableName -> {
                    copyRecordType(
                        RecordType.valueOf(tableName), sourceCollectionId, targetCollectionId);
                  });
          logger.info(
              "... all table schemas created successfully for target collection {}",
              targetCollectionId);

          // now, copy the data from table to table, including the "sys_" join tables.
          logger.info("... copying table data for target collection {}", targetCollectionId);
          tableOrder.forEach(
              tableName -> {
                copyTableData(tableName, sourceCollectionId, targetCollectionId);
              });
          logger.info(
              "... all table data copied successfully for target collection {}",
              targetCollectionId);
        });

    // TODO AJ-1952: populate this better, it should have more info like number of
    //   collections/tables/rows copied
    DefaultCollectionCreationResult result =
        new DefaultCollectionCreationResult(true, new CollectionServerModel("clone", "clone"));

    return result;
  }

  /**
   * Copies the collection definitions - without any tables or data - from one collection to
   * another.
   *
   * @param sourceWorkspaceId workspace from which to copy collections
   * @param targetWorkspaceId workspace into which to copy collections
   * @return map of source collection id -> target collection id
   */
  Map<UUID, UUID> copyCollectionDefinitions(
      WorkspaceId sourceWorkspaceId, WorkspaceId targetWorkspaceId) {

    // init the return object
    Map<UUID, UUID> result = new HashMap<>();

    // get the list of all collections in the source workspace
    List<CollectionServerModel> sourceCollections = collectionService.list(sourceWorkspaceId);

    // for each source collection in the source workspace
    sourceCollections.forEach(
        sourceCollection -> {
          UUID sourceCollectionId = sourceCollection.getId();
          UUID targetCollectionId;

          logger.info("... copying source collection {}", sourceCollectionId);

          // create a corresponding collection in the target workspace
          if (sourceWorkspaceId.id().equals(sourceCollection.getId())) {
            // this is the default collection in the source workspace; create a default collection
            // in the target workspace
            logger.info("... source collection is default; creating target default");
            DefaultCollectionCreationResult creationResult =
                collectionService.createDefaultCollection(targetWorkspaceId);
            targetCollectionId = creationResult.collectionServerModel().getId();
          } else {
            // this is a non-default collection in the source workspace; create a non-default
            // collection in the target workspace
            logger.info(
                "... source collection is non-default; creating target collection '{}'",
                sourceCollection.getName());
            CollectionRequestServerModel collectionRequest =
                new CollectionRequestServerModel(
                    sourceCollection.getName(), sourceCollection.getDescription());
            CollectionServerModel creationResult =
                collectionService.save(targetWorkspaceId, collectionRequest);
            targetCollectionId = creationResult.getId();
          }

          result.put(sourceCollectionId, targetCollectionId);
          logger.info(
              "... source collection {} will clone to target collection {}",
              sourceCollectionId,
              targetCollectionId);
        });

    return result;
  }

  /**
   * Calculates the order in which a collection's tables should be cloned. Since tables use foreign
   * keys, we need to clone the "to" tables in a FK before the "from" tables.
   *
   * @param sourceCollectionId the collection to inspect
   * @return a safe table order
   */
  List<String> getTableOrder(UUID sourceCollectionId) {
    //    list all source db tables in the source collection
    List<String> tableNames = getTableNames(sourceCollectionId);
    //    list all foreign keys in the source collection
    Set<ForeignKeyEdge> foreignKeys = getForeignKeys(sourceCollectionId);
    // instantiate a graph, which will allow us to traverse tables in the proper order.
    // when cloning tables, we need to insert foreign key targets before foreign key
    // sources.
    DirectedAcyclicGraph<String, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);
    // add each table as a node in the graph
    tableNames.forEach(graph::addVertex);
    // add each foreign key as an edge in the graph
    foreignKeys.forEach(
        fkEdge -> {
          graph.addEdge(fkEdge.fromTable, fkEdge.toTable);
        });

    // TODO AJ-1952: what to do about cyclic table structures? One option is to disable FK
    //   validation in Postgres, do the inserts, then turn FK validation back on.

    // traverse the graph in REVERSE topological order. JGraphT's topological order is from
    // the root node to the leaves; we need to insert leaf tables first so they exist
    // before anything that has a foreign key to them. So, we reverse the topological order.
    TopologicalOrderIterator<String, DefaultEdge> topologicalOrderIterator =
        new TopologicalOrderIterator<>(graph);
    List<String> tableOrder = IteratorUtils.toList(topologicalOrderIterator);
    Collections.reverse(tableOrder);

    // finally, move all the "sys_" tables to LAST. We intentionally didn't include these tables
    // when adding edges
    // above, so they aren't in the proper order. We know they should be last.
    Map<Boolean, List<String>> tableGrouping =
        tableOrder.stream()
            .collect(
                Collectors.groupingBy(tableName -> tableName.startsWith(RESERVED_NAME_PREFIX)));

    List<String> userTables = tableGrouping.getOrDefault(false, List.of());
    List<String> sysTables = tableGrouping.getOrDefault(true, List.of());

    return Stream.concat(userTables.stream(), sysTables.stream()).toList();
  }

  /**
   * Copies all row data for a given table from one collection to another collection.
   *
   * <p>Critically, this method uses an "INSERT INTO ... SELECT" sql statement to perform the copy.
   * This means the row data is copied completely within Postgres, and avoids any bulk export and
   * import - no need to pull the data from Postgres to Java and then push it back from Java to
   * Postgres.
   *
   * @param tableName table to copy; this table must exist in both collections
   * @param sourceCollectionId source from which to copy
   * @param targetCollectionId target into which to copy
   */
  void copyTableData(String tableName, UUID sourceCollectionId, UUID targetCollectionId) {
    // copy data from source to target
    // CREATE TABLE copy_table AS SELECT * FROM original_table;
    String copySql =
        "INSERT INTO \"%s\".\"%s\" SELECT * FROM \"%s\".\"%s\";"
            .formatted(targetCollectionId, tableName, sourceCollectionId, tableName);

    namedTemplate.update(copySql, Map.of());

    logger.info("... successfully cloned to table {}.{}", targetCollectionId, tableName);
  }

  /**
   * Creates a copy of a given record type in a new collection.
   *
   * <p>Postgres supports `CREATE TABLE foo (LIKE otherTable INCLUDING ALL)`, but that does NOT
   * handle creating foreign keys ... which we need. So, we do this the long way by asking WDS to
   * re-create the record type in the target based on info from the source.
   *
   * @param recordType the record type to copy
   * @param sourceCollectionId collection being cloned
   * @param targetCollectionId destination into which the record type is copied
   */
  void copyRecordType(RecordType recordType, UUID sourceCollectionId, UUID targetCollectionId) {
    logger.info("... creating table {}.{}", targetCollectionId, recordType);
    // get the existing WDS information from the source table
    Map<String, DataTypeMapping> tableSchema =
        recordDao.getExistingTableSchema(sourceCollectionId, recordType);
    String primaryKey = recordDao.getPrimaryKeyColumn(recordType, sourceCollectionId);
    List<Relation> relations = recordDao.getRelationCols(sourceCollectionId, recordType);
    List<Relation> relationArrays = recordDao.getRelationArrayCols(sourceCollectionId, recordType);

    RelationCollection relationCollection =
        new RelationCollection(Set.copyOf(relations), Set.copyOf(relationArrays));

    // create the record type in the target
    recordDao.createRecordType(
        targetCollectionId, tableSchema, recordType, relationCollection, primaryKey);
    logger.info("... table {}.{} created.", targetCollectionId, recordType);
  }

  /**
   * Get all table names in a given collection. This is similar to RecordDao.getAllRecordTypes, but
   * this method includes sys_ tables as well, since we need to clone the join tables for arrays of
   * relations
   *
   * @param collectionId collection to inspect
   * @return list of table names
   */
  List<String> getTableNames(UUID collectionId) {
    return namedTemplate.queryForList(
        "select tablename from pg_tables WHERE schemaname = :workspaceSchema order by tablename",
        new MapSqlParameterSource("workspaceSchema", collectionId.toString()),
        String.class);
  }

  /**
   * Returns a set of the "from" and "to" tables for all relations/all tables in this collection.
   * Used to build a dependency graph of tables, so we know which tables to clone first.
   *
   * @param collectionId collection to inspect
   * @return set of table-to-table relations
   */
  Set<ForeignKeyEdge> getForeignKeys(UUID collectionId) {
    // TODO AJ-1952: if cloning is a performance bottleneck, this could definitely be optimized. It
    //  currently makes multiple SQL queries per table; we could probably consolidate to 1-2 queries
    //  for the whole collection.
    Set<ForeignKeyEdge> fks = new HashSet<>();

    recordDao
        .getAllRecordTypes(collectionId)
        .forEach(
            recType -> {
              List<Relation> relations = recordDao.getRelationCols(collectionId, recType);
              List<Relation> relationArrays = recordDao.getRelationArrayCols(collectionId, recType);

              relations.addAll(relationArrays);

              relations.forEach(
                  relation -> {
                    fks.add(
                        new ForeignKeyEdge(
                            recType.getName(), relation.relationRecordType().getName()));
                  });
            });

    return fks;
  }

  // helper class for foreign keys
  record ForeignKeyEdge(String fromTable, String toTable) {}
}
