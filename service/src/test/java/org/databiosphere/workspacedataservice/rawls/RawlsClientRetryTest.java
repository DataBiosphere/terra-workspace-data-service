package org.databiosphere.workspacedataservice.rawls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.DataRepoSnapshotResource;
import com.google.api.client.http.HttpStatusCodes;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpEntity;
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
@ActiveProfiles(
    value = {"control-plane", "test"},
    inheritProfiles = false)
@SpringBootTest
@TestPropertySource(
    properties = {
      // URI parsing requires a valid hostname here, even if we don't contact this host
      "rawlsUrl=https://localhost/",
      // allow 3 retry attempts, so we can better verify retries
      "rest.retry.maxAttempts=3",
      // with aggressive delay settings so unit tests don't run too long
      "rest.retry.backoff.delay=3",
    })
class RawlsClientRetryTest {
  // create mock for RestTemplate
  @MockBean RestTemplate mockRestTemplate;

  @Autowired RawlsClient rawlsClient;

  @BeforeEach
  void beforeEach() {
    // reset mockRestTemplate
    reset(mockRestTemplate);
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
    ResponseEntity<SnapshotListResponse> successResponse =
        ResponseEntity.of(Optional.of(new SnapshotListResponse(List.of())));

    // argument matcher for the rest template target url
    var uriMatcher = new UriMatcher("/api/workspaces/%s/snapshots/v2".formatted(workspaceId));

    // RestTemplate will throw 502 on the first and second attempts, but succeed on the third
    when(mockRestTemplate.exchange(
            argThat(uriMatcher), eq(HttpMethod.GET), any(), eq(SnapshotListResponse.class)))
        .thenThrow(badGateway)
        .thenThrow(badGateway)
        .thenReturn(successResponse);

    // ACT
    // execute the method under test
    SnapshotListResponse snapshotListResponse =
        rawlsClient.enumerateDataRepoSnapshotReferences(workspaceId.id(), 0, 5);

    // ASSERT
    // verify it retried three times, until it got the success
    verify(mockRestTemplate, times(3))
        .exchange(argThat(uriMatcher), eq(HttpMethod.GET), any(), eq(SnapshotListResponse.class));

    assertThat(snapshotListResponse.gcpDataRepoSnapshots()).isEmpty();
  }

  @Test
  void linkDoesRetry() {
    // ARRANGE
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    UUID snapshotId = UUID.randomUUID();

    // argument matcher for the rest template target url
    var uriMatcher = new UriMatcher("/api/workspaces/%s/snapshots/v2".formatted(workspaceId));
    // argument matcher for the post payload
    var payloadMatcher = new NamedDataRepoSnapshotHttpEntityMatcher(snapshotId);

    // RestTemplate will throw 502 on the first and second attempts, but succeed on the third
    when(mockRestTemplate.exchange(
            argThat(uriMatcher),
            eq(HttpMethod.POST),
            argThat(payloadMatcher),
            eq(DataRepoSnapshotResource.class)))
        .thenThrow(badGateway)
        .thenThrow(badGateway)
        .thenAnswer((Answer<?>) invocation -> null);

    // ACT
    // execute the method under test
    rawlsClient.createSnapshotReference(workspaceId.id(), snapshotId);

    // ASSERT
    // verify it retried three times, until it got the success
    verify(mockRestTemplate, times(3))
        .exchange(
            argThat(uriMatcher),
            eq(HttpMethod.POST),
            argThat(payloadMatcher),
            eq(DataRepoSnapshotResource.class));
  }

  // custom ArgumentMatcher - does a given URI contain the supplied substring?
  static class UriMatcher implements ArgumentMatcher<URI> {
    private final String substring;

    UriMatcher(String substring) {
      this.substring = substring;
    }

    @Override
    public boolean matches(URI argument) {
      return argument.toString().contains(substring);
    }
  }

  // custom ArgumentMatcher - does a given NamedDataRepoSnapshot inside a HttpEntity
  // contain the supplied snapshotId?
  static class NamedDataRepoSnapshotHttpEntityMatcher
      implements ArgumentMatcher<HttpEntity<NamedDataRepoSnapshot>> {
    private final UUID snapshotId;

    NamedDataRepoSnapshotHttpEntityMatcher(UUID snapshotId) {
      this.snapshotId = snapshotId;
    }

    @Override
    public boolean matches(HttpEntity<NamedDataRepoSnapshot> argument) {

      return snapshotId.equals(argument.getBody().snapshotId());
    }
  }
}
