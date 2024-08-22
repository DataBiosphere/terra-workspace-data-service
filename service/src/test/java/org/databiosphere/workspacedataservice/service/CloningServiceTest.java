package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.generated.CollectionRequestServerModel;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.service.CloningService.ForeignKeyEdge;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.attributes.RelationAttribute;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@ActiveProfiles("control-plane")
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // disable virtual collections
      "twds.tenancy.allow-virtual-collections=false",
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/"
    })
@DirtiesContext
public class CloningServiceTest {

  @Autowired CloningService cloningService;
  @Autowired CollectionService collectionService;
  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @Autowired RecordService recordService;
  @Autowired WorkspaceService workspaceService;

  @BeforeEach
  @AfterEach
  void cleanup() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
  }

  // this tests the behavior of the JGraphT library. We don't need this test - I made it to help ME
  // understand how the library works!
  @Test
  void graphOrdering() {

    List<String> tableNames = List.of("red", "green", "blue");
    Map<String, String> foreignKeys = Map.of("green", "red");

    // instantiate a graph, which will allow us to traverse tables in the proper order.
    // when processing tables, we need to insert foreign key targets before foreign key
    // sources.
    DirectedAcyclicGraph<String, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);

    // add each table as a node in the graph
    tableNames.forEach(graph::addVertex);
    // add each foreign key as an edge in the graph
    foreignKeys.forEach(graph::addEdge);

    // traverse the graph in topological order
    TopologicalOrderIterator<String, DefaultEdge> topologicalOrderIterator =
        new TopologicalOrderIterator<>(graph);

    // for the purpose of assertions, create a map of vertex->index in list
    Map<String, Integer> vertexIndices = new HashMap<>();
    int i = 0;
    while (topologicalOrderIterator.hasNext()) {
      String vertex = topologicalOrderIterator.next();
      vertexIndices.put(vertex, i);
      i++;
    }

    // expected ordering:
    // there exists an edge green->red, so green must come before red
    assertThat(vertexIndices.get("green")).isLessThan(vertexIndices.get("red"));
  }

  @Test
  void getTableNamesTest() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collection =
        collectionService.save(workspaceId, new CollectionRequestServerModel("unit", "test"));
    CollectionId collectionId = CollectionId.of(collection.getId());

    RecordType red = RecordType.valueOf("red");
    RecordType green = RecordType.valueOf("green");
    RecordType blue = RecordType.valueOf("blue");

    // create record type "red" and record "red-one"
    recordService.upsertSingleRecord(
        collectionId.id(),
        red,
        "red-one",
        Optional.empty(),
        new RecordRequest(RecordAttributes.empty()));

    // create record type "blue" and record "blue-one" with a scalar relation to red
    recordService.upsertSingleRecord(
        collectionId.id(),
        blue,
        "blue-one",
        Optional.empty(),
        new RecordRequest(
            new RecordAttributes(Map.of("rel", new RelationAttribute(red, "red-one")))));

    // add table green, with an array of relations to blue
    recordService.upsertSingleRecord(
        collectionId.id(),
        green,
        "green-one",
        Optional.empty(),
        new RecordRequest(
            new RecordAttributes(Map.of("rel", List.of(new RelationAttribute(blue, "blue-one"))))));

    // list table names
    List<String> actual = cloningService.getTableNames(collectionId.id());

    // should be red, green, blue, and join table from green to blue
    Set<String> expected = Set.of("red", "green", "blue", "sys_green_rel");

    assertEquals(expected, Set.copyOf(actual));
  }

  @Test
  void getForeignKeysTest() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionServerModel collection =
        collectionService.save(workspaceId, new CollectionRequestServerModel("unit", "test"));
    CollectionId collectionId = CollectionId.of(collection.getId());

    RecordType red = RecordType.valueOf("red");
    RecordType green = RecordType.valueOf("green");
    RecordType blue = RecordType.valueOf("blue");

    // create record type "red" and record "red-one"
    recordService.upsertSingleRecord(
        collectionId.id(),
        red,
        "red-one",
        Optional.empty(),
        new RecordRequest(RecordAttributes.empty()));

    // create record type "blue" and record "blue-one" with a scalar relation to red
    recordService.upsertSingleRecord(
        collectionId.id(),
        blue,
        "blue-one",
        Optional.empty(),
        new RecordRequest(
            new RecordAttributes(Map.of("rel", new RelationAttribute(red, "red-one")))));

    // add table green, with an array of relations to blue
    recordService.upsertSingleRecord(
        collectionId.id(),
        green,
        "green-one",
        Optional.empty(),
        new RecordRequest(
            new RecordAttributes(Map.of("rel", List.of(new RelationAttribute(blue, "blue-one"))))));

    // list foreign keys
    List<ForeignKeyEdge> actual = cloningService.getForeignKeys(collectionId.id());

    // we expect relations of:
    // blue->red
    // green->blue
    List<ForeignKeyEdge> expected =
        List.of(new ForeignKeyEdge("blue", "red"), new ForeignKeyEdge("green", "blue"));

    assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
  }
}
