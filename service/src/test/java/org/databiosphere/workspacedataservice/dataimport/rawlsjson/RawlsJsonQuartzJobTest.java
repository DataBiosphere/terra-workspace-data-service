package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonQuartzJob.rawlsJsonBlobName;
import static org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonQuartzJobTest.INCOMING_BUCKET;
import static org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonTestSupport.stubJobContext;
import static org.mockito.Mockito.verify;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.sam.MockSamUsersApi;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@DirtiesContext
@SpringBootTest
@ActiveProfiles(
    value = {"mock-sam", "control-plane"},
    inheritProfiles = false)
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      "twds.data-import.rawls-json-direct-import-bucket=" + INCOMING_BUCKET,
      "rawlsUrl=https://localhost/",
    })
class RawlsJsonQuartzJobTest extends TestBase {
  static final String INCOMING_BUCKET = "allowed-bucket";
  @Autowired private RawlsJsonTestSupport testSupport;
  @SpyBean private PubSub pubSub;
  @MockBean private JobDao jobDao;

  private GcsStorage incomingStorage;

  @Autowired
  @Qualifier("mockGcsStorage")
  private GcsStorage outgoingStorage;

  /** ArgumentCaptor for the message passed to {@link PubSub#publishSync(Map)}. */
  @Captor private ArgumentCaptor<Map<String, String>> pubSubMessageCaptor;

  private CollectionId collectionId;

  @BeforeEach
  void setup() {
    collectionId = CollectionId.of(UUID.randomUUID());
    incomingStorage = new GcsStorage(LocalStorageHelper.getOptions().getService(), INCOMING_BUCKET);
  }

  @AfterEach
  void teardown() {
    deleteAllBlobs(incomingStorage);
    deleteAllBlobs(outgoingStorage);
  }

  @ParameterizedTest(name = "isUpsert should be passed through to pubsub ({0})")
  @ValueSource(booleans = {true, false})
  void passesThroughIsUpsert(boolean isUpsert) throws JobExecutionException {
    // arrange
    UUID jobId = UUID.randomUUID();
    Blob incoming = createRandomBlob();
    JobExecutionContext mockContext =
        stubJobContext(jobId, getUri(incoming), collectionId.id(), isUpsert);

    // act
    testSupport.buildRawlsJsonQuartzJob().execute(mockContext);

    // assert
    assertSingleBlobWritten(rawlsJsonBlobName(jobId));
    assertPubSubMessage(expectedPubSubMessageFor(jobId, isUpsert));
  }

  private ImmutableMap<String, String> expectedPubSubMessageFor(UUID jobId, boolean isUpsert) {
    return new ImmutableMap.Builder<String, String>()
        .put("workspaceId", collectionId.toString())
        .put("userEmail", MockSamUsersApi.MOCK_USER_EMAIL)
        .put("jobId", jobId.toString())
        .put("upsertFile", outgoingStorage.getBucketName() + "/" + rawlsJsonBlobName(jobId))
        .put("isUpsert", String.valueOf(isUpsert))
        .put("isCWDS", "true")
        .build();
  }

  private void assertPubSubMessage(Map<String, String> expectedMessage) {
    verify(pubSub).publishSync(pubSubMessageCaptor.capture());
    assertThat(pubSubMessageCaptor.getValue()).isEqualTo(expectedMessage);
  }

  private void assertSingleBlobWritten(String expectedBlobName) {
    List<Blob> blobsWritten = listBlobs(outgoingStorage);
    assertThat(blobsWritten).hasSize(1);
    assertThat(blobsWritten.get(0).getName()).isEqualTo(expectedBlobName);
  }

  private List<Blob> listBlobs(GcsStorage storage) {
    return stream(storage.getBlobsInBucket().spliterator(), /* parallel= */ false).toList();
  }

  private Blob createRandomBlob() {
    return incomingStorage.createBlob(UUID.randomUUID().toString());
  }

  private URI getUri(Blob blob) {
    return URI.create(blob.getBlobId().toGsUtilUri());
  }

  private void deleteAllBlobs(GcsStorage storage) {
    storage.getBlobsInBucket().forEach(blob -> storage.deleteBlob(blob.getName()));
  }
}
