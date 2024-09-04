package org.databiosphere.workspacedataservice.rawls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpStatusCodes;
import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.HttpServerErrorException;

@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      // allow 3 retry attempts, so we can better verify retries
      "rest.retry.maxAttempts=3",
      // with aggressive delay settings so unit tests don't run too long
      "rest.retry.backoff.delay=3",
    })
class RawlsClientRetryTest extends ControlPlaneTestBase {
  // create mock for RawlsApi
  @MockBean RawlsApi mockRawlsApi;

  @Autowired RawlsClient rawlsClient;

  @BeforeEach
  void beforeEach() {
    // reset mockRawlsApi
    reset(mockRawlsApi);
  }

  // define a 502 error, thrown by mocks below
  Exception badGateway =
      HttpServerErrorException.create(
          HttpStatusCode.valueOf(HttpStatusCodes.STATUS_CODE_BAD_GATEWAY),
          "unit test bad gateway",
          HttpHeaders.EMPTY,
          new byte[] {},
          null);

  @Test
  void enumerateDoesRetry() {
    // ARRANGE
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // define the successful REST response payload
    SnapshotListResponse successResponse = new SnapshotListResponse(List.of());

    // RestTemplate will throw 502 on the first and second attempts, but succeed on the third
    when(mockRawlsApi.enumerateDataRepoSnapshotByWorkspaceId(workspaceId.id(), 0, 5))
        .thenThrow(badGateway)
        .thenThrow(badGateway)
        .thenReturn(successResponse);

    // ACT
    // execute the method under test
    SnapshotListResponse snapshotListResponse =
        rawlsClient.enumerateDataRepoSnapshotReferences(workspaceId.id(), 0, 5);

    // ASSERT
    // verify it retried three times, until it got the success
    verify(mockRawlsApi, times(3)).enumerateDataRepoSnapshotByWorkspaceId(workspaceId.id(), 0, 5);

    assertThat(snapshotListResponse.gcpDataRepoSnapshots()).isEmpty();
  }

  @Test
  void linkDoesRetry() {
    // ARRANGE
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    UUID snapshotId = UUID.randomUUID();

    // argument matcher for the post payload
    var payloadMatcher = new NamedDataRepoSnapshotHttpEntityMatcher(snapshotId);

    // RestTemplate will throw 502 on the first and second attempts, but succeed on the third
    when(mockRawlsApi.createDataRepoSnapshotByWorkspaceId(
            eq(workspaceId.id()), argThat(payloadMatcher)))
        .thenThrow(badGateway)
        .thenThrow(badGateway)
        .thenAnswer((Answer<?>) invocation -> null);

    // ACT
    // execute the method under test
    rawlsClient.createSnapshotReference(workspaceId.id(), snapshotId);

    // ASSERT
    // verify it retried three times, until it got the success
    verify(mockRawlsApi, times(3))
        .createDataRepoSnapshotByWorkspaceId(eq(workspaceId.id()), argThat(payloadMatcher));
  }

  // custom ArgumentMatcher - does a given NamedDataRepoSnapshot
  // contain the supplied snapshotId?
  // we need this because RawlsClient.createSnapshotReference adds a timestamp to each
  // reference it creates, and those are hard to predict
  static class NamedDataRepoSnapshotHttpEntityMatcher
      implements ArgumentMatcher<NamedDataRepoSnapshot> {
    private final UUID snapshotId;

    NamedDataRepoSnapshotHttpEntityMatcher(UUID snapshotId) {
      this.snapshotId = snapshotId;
    }

    @Override
    public boolean matches(NamedDataRepoSnapshot argument) {

      return snapshotId.equals(argument.snapshotId());
    }
  }
}
