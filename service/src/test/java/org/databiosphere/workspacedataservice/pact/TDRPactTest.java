package org.databiosphere.workspacedataservice.pact;

import static org.junit.jupiter.api.Assertions.assertThrows;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import java.util.UUID;
import org.databiosphere.workspacedataservice.datarepo.DataRepoClientFactory;
import org.databiosphere.workspacedataservice.datarepo.DataRepoDao;
import org.databiosphere.workspacedataservice.datarepo.DataRepoException;
import org.databiosphere.workspacedataservice.datarepo.HttpDataRepoClientFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Tag("pact-test")
@ExtendWith(PactConsumerTestExt.class)
class TDRPactTest {

  @BeforeEach
  void setUp() {
    // Without this setup, the HttpClient throws a "No thread-bound request found" error
    MockHttpServletRequest request = new MockHttpServletRequest();
    // Set the mock request as the current request context
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
  }

  static final String dummySnapshotId = "12345678-abc9-012d-3456-e7fab89cd01e";

  @Pact(consumer = "wds-consumer", provider = "tdr-provider")
  public RequestResponsePact noSnapshotPact(PactDslWithProvider builder) {
    return builder
        .given("snapshot doesn't exist")
        .uponReceiving("a snapshot request")
        .pathFromProviderState(
            "/api/repository/v1/snapshots/${dummySnapshotId}",
            String.format("/api/repository/v1/snapshots/%s", dummySnapshotId))
        .method("GET")
        .willRespondWith()
        .status(404)
        .body("")
        .toPact();
  }

  @Test
  @PactTestFor(pactMethod = "noSnapshotPact", pactVersion = PactSpecVersion.V3)
  void testNoSnapshot(MockServer mockServer) {
    DataRepoClientFactory clientFactory = new HttpDataRepoClientFactory(mockServer.getUrl());
    DataRepoDao dataRepoDao = new DataRepoDao(clientFactory);

    assertThrows(
        DataRepoException.class,
        () -> dataRepoDao.getSnapshot(UUID.fromString(dummySnapshotId)),
        "nonexistent snapshot should return 404");
  }
}
