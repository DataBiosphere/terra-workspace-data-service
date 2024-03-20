package org.databiosphere.workspacedataservice.dataimport.snapshotsupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.ResourceList;
import com.google.api.client.http.HttpStatusCodes;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.rawls.SnapshotListResponse;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

@DirtiesContext
@ActiveProfiles(value = "control-plane", inheritProfiles = false)
@SpringBootTest
@TestPropertySource(
    properties = {
      // URI parsing requires a valid hostname here, even if we don't contact this host
      "rawlsUrl=https://localhost/",
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // allow 3 retry attempts, so we can better verify retries
      "rest.retry.maxAttempts=3",
      // with aggressive delay settings so unit tests don't run too long
      "rest.retry.backoff.delay=3",
    })
public class RawlsSnapshotSupportRetryTest {

  // create mock for RestTemplate
  @MockBean RestTemplate mockRestTemplate;

  @Autowired SnapshotSupportFactory snapshotSupportFactory;

  @Test
  void enumerateDoesRetry() {
    // ARRANGE
    // get the RawlsSnapshotSupport
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    SnapshotSupport snapshotSupport = snapshotSupportFactory.buildSnapshotSupport(workspaceId);

    // define the successful REST response payload
    ResponseEntity<SnapshotListResponse> successResponse =
        ResponseEntity.of(Optional.of(new SnapshotListResponse(List.of())));
    // define a 502 error
    Exception badGateway =
        HttpServerErrorException.create(
            HttpStatusCode.valueOf(HttpStatusCodes.STATUS_CODE_BAD_GATEWAY),
            "unit test bad gateway",
            HttpHeaders.EMPTY,
            new byte[] {},
            null);

    // RestTemplate will throw 502 on the first and second attempts, but succeed on the third
    when(mockRestTemplate.exchange(
            any(), eq(HttpMethod.GET), any(), eq(SnapshotListResponse.class)))
        .thenThrow(badGateway)
        .thenThrow(badGateway)
        .thenReturn(successResponse);

    // ACT
    // execute the method under test
    ResourceList resourceList = snapshotSupport.enumerateDataRepoSnapshotReferences(0, 5);

    // ASSERT
    // verify it retried three times, until it got the success
    verify(mockRestTemplate, times(3))
        .exchange(any(), eq(HttpMethod.GET), any(), eq(SnapshotListResponse.class));

    assertThat(resourceList.getResources()).isEmpty();
  }
}
