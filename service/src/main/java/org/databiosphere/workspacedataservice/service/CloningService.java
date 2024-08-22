package org.databiosphere.workspacedataservice.service;

import bio.terra.common.db.WriteTransaction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.collections4.IteratorUtils;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.shared.model.DefaultCollectionCreationResult;
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

  private static final Logger logger = LoggerFactory.getLogger(CloningService.class);

  public CloningService(
      CollectionService collectionService, NamedParameterJdbcTemplate namedTemplate) {
    this.collectionService = collectionService;
    this.namedTemplate = namedTemplate;
  }

  @WriteTransaction
  public DefaultCollectionCreationResult clone(
      WorkspaceId sourceWorkspaceId, WorkspaceId targetWorkspaceId) {

    logger.info(
        "Starting clone from workspace {} to workspace {}", sourceWorkspaceId, targetWorkspaceId);

    // get the list of all collections in the source workspace
    List<CollectionServerModel> sourceCollections = collectionService.list(sourceWorkspaceId);

    // for each source collection in the source workspace
    sourceCollections.forEach(
        sourceCollection -> {
          UUID sourceCollectionId = sourceCollection.getId();
          UUID targetCollectionId;

          logger.info("... processing source collection {}", sourceCollectionId);

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
          logger.info(
              "... source collection {} will clone to target collection {}",
              sourceCollectionId,
              targetCollectionId);

          //    list all source db tables in the source collection
          List<String> tableNames = getTableNames(sourceCollection.getId());
          //    list all foreign keys in the source collection
          List<ForeignKeyEdge> foreignKeys = getForeignKeys(sourceCollection.getId());
          // instantiate a graph, which will allow us to traverse tables in the proper order.
          // when cloning tables, we need to insert foreign key targets before foreign key
          // sources.
          DirectedAcyclicGraph<String, DefaultEdge> graph =
              new DirectedAcyclicGraph<>(DefaultEdge.class);
          // add each table as a node in the graph
          tableNames.forEach(graph::addVertex);
          // add each foreign key as an edge in the graph
          foreignKeys.forEach(fkEdge -> graph.addEdge(fkEdge.fromTable, fkEdge.toTable));

          // TODO AJ-1952: what to do about cyclic table structures? One option is to disable FK
          //   validation in Postgres, do the inserts, then turn FK validation back on.

          // traverse the graph in REVERSE topological order. JGraphT's topological order is from
          // the root node to the leaves; we need to insert leaf tables first so they exist
          // before anything that has a foreign key to them. So, we reverse the topo order.
          TopologicalOrderIterator<String, DefaultEdge> topologicalOrderIterator =
              new TopologicalOrderIterator<>(graph);
          List<String> topoOrder = IteratorUtils.toList(topologicalOrderIterator);
          Collections.reverse(topoOrder);

          topoOrder.forEach(
              tableName -> {
                logger.info("... processing table {}.{}", sourceCollectionId, tableName);

                // create table in target
                // CREATE TABLE target_table (LIKE source_table INCLUDING ALL);
                // TODO AJ-1952: this does NOT include foreign key constraints!
                String createSql =
                    "CREATE TABLE \"%s\".\"%s\" (LIKE \"%s\".\"%s\" INCLUDING ALL);"
                        .formatted(targetCollectionId, tableName, sourceCollectionId, tableName);

                namedTemplate.update(createSql, Map.of());

                // copy data from source to target
                // CREATE TABLE copy_table AS SELECT * FROM original_table;
                String copySql =
                    "INSERT INTO \"%s\".\"%s\" SELECT * FROM \"%s\".\"%s\";"
                        .formatted(targetCollectionId, tableName, sourceCollectionId, tableName);

                namedTemplate.update(copySql, Map.of());

                logger.info(
                    "... successfully cloned to table {}.{}", targetCollectionId, tableName);
              });
        });

    // TODO AJ-1952: populate this better, it should have more info like number of
    //   collections/tables/rows copied
    DefaultCollectionCreationResult result =
        new DefaultCollectionCreationResult(true, new CollectionServerModel("clone", "clone"));

    return result;
  }

  // similar to RecordDao.getAllRecordTypes, but this method includes sys_ tables as well, since
  // we need to clone the join tables for arrays of relations
  List<String> getTableNames(UUID collectionId) {
    return namedTemplate.queryForList(
        "select tablename from pg_tables WHERE schemaname = :workspaceSchema order by tablename",
        new MapSqlParameterSource("workspaceSchema", collectionId.toString()),
        String.class);
  }

  /**
   * Returns a map of FK target->FK source. In the graph we're foreign keys - graph edges - are
   * added IN REVERSE. If a table "red" has a foreign key to table "green", we add an edge of
   * green->red. This allows the topological graph sort to function in the order we need.
   *
   * <p>Similar to RecordDao.getRelationCols, but this method operates on all tables in the
   * collection and only returns table information, not column information
   *
   * @param collectionId collection to inspect
   * @return list of table-to-table relations
   */
  // TODO AJ-1952: implement
  List<ForeignKeyEdge> getForeignKeys(UUID collectionId) {
    return namedTemplate.query(
        "SELECT tc.table_name as from_table, ccu.table_name as to_table "
            + "FROM information_schema.table_constraints tc "
            + "JOIN information_schema.constraint_column_usage ccu "
            + "ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema "
            + "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = :collectionId",
        Map.of("collectionId", collectionId.toString()),
        (rs, rowNum) -> new ForeignKeyEdge(rs.getString("from_table"), rs.getString("to_table")));
  }

  record ForeignKeyEdge(String fromTable, String toTable) {}
}
