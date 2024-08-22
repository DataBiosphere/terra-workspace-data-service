package org.databiosphere.workspacedataservice.service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.service.model.Relation;
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

  private static final Logger logger = LoggerFactory.getLogger(CloningService.class);

  public CloningService(
      CollectionService collectionService, NamedParameterJdbcTemplate namedTemplate) {
    this.collectionService = collectionService;
    this.namedTemplate = namedTemplate;
  }

  @SuppressWarnings("UnstableApiUsage") // Guava's Graph feature is marked @Beta
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
                "... source collection is non-default; creating target collection '{}",
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
          Map<String, String> foreignKeys = getForeignKeys(sourceCollection.getId());
          // instantiate a graph, which will allow us to traverse tables in the proper order.
          // when processing tables, we need to insert foreign key targets before foreign key
          // sources.
          DirectedAcyclicGraph<String, DefaultEdge> graph =
              new DirectedAcyclicGraph<>(DefaultEdge.class);
          // add each table as a node in the graph
          tableNames.forEach(graph::addVertex);
          // add each foreign key as an edge in the graph
          foreignKeys.forEach(graph::addEdge);

          // traverse the graph in topological order
          TopologicalOrderIterator<String, DefaultEdge> topologicalOrderIterator =
              new TopologicalOrderIterator<>(graph);

          while (topologicalOrderIterator.hasNext()) {
            String tableName = topologicalOrderIterator.next();

            logger.info("... processing table {}.{}", sourceCollectionId, tableName);

            // copy table from source to target
            // CREATE TABLE copy_table AS SELECT * FROM original_table;
            String sql =
                "CREATE TABLE \"%s\".\"%s\" AS SELECT * FROM \"%s\".\"%s\""
                    .formatted(targetCollectionId, tableName, sourceCollectionId, tableName);

            namedTemplate.update(sql, Map.of());

            logger.info("... successfully cloned to table {}.{}", targetCollectionId, tableName);
          }
        });

    // TODO AJ-1952: populate this better
    DefaultCollectionCreationResult result =
        new DefaultCollectionCreationResult(true, new CollectionServerModel("clone", "clone"));

    return result;
  }

  // similar to RecordDao.getAllRecordTypes, but this method includes sys_ tables as well, since
  // we need to clone the join tables for arrays of relations
  private List<String> getTableNames(UUID collectionId) {
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
   * collection
   *
   * @param collectionId
   * @return
   */
  // TODO AJ-1952: implement
  private Map<String, String> getForeignKeys(UUID collectionId) {
    return Map.of();
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
}
