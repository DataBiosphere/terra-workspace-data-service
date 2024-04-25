package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonQuartzJob.rawlsJsonBlobName;
import static org.databiosphere.workspacedataservice.dataimport.rawlsjson.RawlsJsonQuartzJobControlPlaneE2ETest.INCOMING_BUCKET;
import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.RAWLSJSON;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_COLLECTION;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.collect.ImmutableMap;
import io.micrometer.observation.ObservationRegistry;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.ImportDetailsRetriever;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import org.databiosphere.workspacedataservice.pubsub.PubSub;
import org.databiosphere.workspacedataservice.sam.MockSamUsersApi;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * Tests for RAWLSJSON import that execute "end-to-end" - which for this format is relatively
 * simple, merely moving the import file from the given URI to an expected location without any
 * reprocessing and communicating that the new location to Rawls via pubsub.
 */
@ActiveProfiles(profiles = {"mock-sam", "noop-scheduler-dao", "control-plane"})
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // Allow file imports to test with files from resources.
      "twds.data-import.allowed-schemes=file",
      "twds.data-import.rawls-json-direct-import-bucket=" + INCOMING_BUCKET,
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/",
    })
@AutoConfigureMockMvc
class RawlsJsonQuartzJobControlPlaneE2ETest {
  static final String INCOMING_BUCKET = "allowed-bucket";

  @Autowired private ImportService importService;
  @Autowired private DataImportProperties dataImportProperties;
  @Autowired private ObservationRegistry observations;
  @Autowired private JobDao jobDao;
  @Autowired private ImportDetailsRetriever importDetailsRetriever;
  @SpyBean private PubSub pubSub;

  GcsStorage incomingStorage;

  @Autowired
  @Qualifier("mockGcsStorage")
  GcsStorage outgoingStorage;

  /** ArgumentCaptor for the message passed to {@link PubSub#publishSync(Map)}. */
  @Captor private ArgumentCaptor<Map<String, String>> pubSubMessageCaptor;

  private UUID collectionId;

  @BeforeEach
  void setup() {
    collectionId = UUID.randomUUID();
    incomingStorage = new GcsStorage(LocalStorageHelper.getOptions().getService(), INCOMING_BUCKET);
  }

  @AfterEach
  void teardown() {
    outgoingStorage.getBlobsInBucket().forEach(blob -> outgoingStorage.deleteBlob(blob.getName()));
  }

  @Test
  void happyPath() throws JobExecutionException {
    // Arrange
    URI incomingBlobUri = getUri(createRandomBlob());
    var importRequest = new ImportRequestServerModel(RAWLSJSON, incomingBlobUri);

    var genericJobServerModel = importService.createImport(collectionId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext =
        stubJobContext(jobId, incomingBlobUri, collectionId, /* isUpsert= */ true);

    // Act
    buildRawlsJsonQuartzJob().execute(mockContext);

    // Assert
    assertPubSubMessage(expectedPubSubMessageFor(jobId, /* isUpsert= */ true));
    assertSingleBlobWritten(rawlsJsonBlobName(jobId));
  }

  private RawlsJsonQuartzJob buildRawlsJsonQuartzJob() {
    return new RawlsJsonQuartzJob(
        dataImportProperties,
        observations,
        jobDao,
        importDetailsRetriever,
        outgoingStorage,
        pubSub);
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

  public static JobExecutionContext stubJobContext(
      UUID jobId, URI resourceUri, UUID collectionId, boolean isUpsert) {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);

    var schedulable =
        ImportService.createSchedulable(
            TypeEnum.RAWLSJSON,
            jobId,
            new ImmutableMap.Builder<String, Serializable>()
                .put(ARG_TOKEN, "bearerToken")
                .put(ARG_URL, resourceUri.toString())
                .put(ARG_COLLECTION, collectionId.toString())
                .put("isUpsert", String.valueOf(isUpsert))
                .build());

    JobDetail jobDetail = schedulable.getJobDetail();
    when(mockContext.getMergedJobDataMap()).thenReturn(jobDetail.getJobDataMap());
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    return mockContext;
  }

  private void assertPubSubMessage(Map<String, String> expectedMessage) {
    verify(pubSub).publishSync(pubSubMessageCaptor.capture());
    assertThat(pubSubMessageCaptor.getValue()).isEqualTo(expectedMessage);
  }

  private void assertSingleBlobWritten(String expectedBlobName) {
    var blobsWritten =
        StreamSupport.stream(
                outgoingStorage.getBlobsInBucket().spliterator(), /* parallel= */ false)
            .toList();
    assertThat(blobsWritten).hasSize(1);
    assertThat(blobsWritten.get(0).getName()).isEqualTo(expectedBlobName);
  }

  private Blob createRandomBlob() {
    return incomingStorage.createBlob(UUID.randomUUID().toString());
  }

  private URI getUri(Blob blob) {
    return URI.create(blob.getBlobId().toGsUtilUri());
  }
}
