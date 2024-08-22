package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.databiosphere.workspacedataservice.TestUtils;
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
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/"
    })
@DirtiesContext
public class CloningServiceTest {

  @Autowired CollectionService collectionService;
  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @Autowired RecordService recordService;
  @Autowired WorkspaceService workspaceService;

  @BeforeEach
  @AfterEach
  void cleanup() {}

  @Test
  void graphOrdering() {

    List<String> tableNames = List.of("red", "green", "blue");
    // foreign keys - graph edges - are added IN REVERSE. If a table "red" has a foreign key to
    // table "green", we add an edge of green->red. This allows the topological graph sort to
    // function in the order we need.
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
}
