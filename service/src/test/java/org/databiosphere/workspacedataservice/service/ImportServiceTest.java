package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestImportOptions.OPTION_TDR_SYNC_PERMISSIONS;
import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import static org.databiosphere.workspacedataservice.service.ImportService.ARG_IMPORT_JOB_INPUT;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_COLLECTION;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.SchedulerDao;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbQuartzJob;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestImportOptions;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestQuartzJob;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.Schedulable;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

@DirtiesContext
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(properties = "twds.data-import.connectivity-check-enabled=false")
class ImportServiceTest extends ControlPlaneTestBase {

  @Autowired ImportService importService;
  @Autowired CollectionService collectionService;
  @MockitoSpyBean JobDao jobDao;
  @MockitoBean SchedulerDao schedulerDao;
  @MockitoBean SamClientFactory mockSamClientFactory;
  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @Autowired WorkspaceRepository workspaceRepository;

  private WorkspaceId workspaceId;

  /** ArgumentCaptor for the Schedulable passed to {@link SchedulerDao#schedule(Schedulable)}. */
  @Captor private ArgumentCaptor<Schedulable> schedulableCaptor;

  GoogleApi mockSamGoogleApi = Mockito.mock(GoogleApi.class);

  private final URI importUri = URI.create("https://anvil.gi.ucsc.edu/testcontainer/path/to/file");

  @BeforeAll
  void beforeAll() {
    // reset to zero collections
    TestUtils.cleanAllCollections(collectionService, namedTemplate);

    // create the WDS-powered workspace
    workspaceId = WorkspaceId.of(UUID.randomUUID());
    workspaceRepository.save(
        new WorkspaceRecord(workspaceId, WorkspaceDataTableType.WDS, /* newFlag= */ true));

    // create the default collection
    collectionService.createDefaultCollection(workspaceId);
  }

  @BeforeEach
  void setUp() throws ApiException {
    // return the mock ResourcesApi from the mock SamClientFactory
    given(mockSamClientFactory.getGoogleApi(any(BearerToken.class))).willReturn(mockSamGoogleApi);
    // Pet token request returns "arbitraryToken"
    reset(mockSamGoogleApi);
    given(mockSamGoogleApi.getArbitraryPetServiceAccountToken(any())).willReturn("arbitraryToken");
  }

  @AfterAll
  void afterAll() {
    // reset to zero collections
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
    // reset to zero workspaces
    TestUtils.cleanAllWorkspaces(namedTemplate);
  }

  // does createSchedulable properly store the jobId, job group, and job data map?
  @ParameterizedTest(name = "for import type {0}")
  @EnumSource(
      value = TypeEnum.class,
      names = {"RAWLSJSON"},
      mode = Mode.EXCLUDE)
  void createSchedulableValues(TypeEnum importType) {
    UUID jobId = UUID.randomUUID();
    Map<String, Serializable> arguments = Map.of("foo", "bar", "twenty-three", 23);

    Schedulable actual = ImportService.createSchedulable(importType, jobId, arguments);

    JobDataMap expectedJobDataMap = new JobDataMap();
    expectedJobDataMap.put("foo", "bar");
    expectedJobDataMap.put("twenty-three", 23);

    assertEquals(jobId.toString(), actual.getId());
    assertEquals(importType.name(), actual.getGroup());

    assertThat(actual.getJobDetail().getJobDataMap()).isEqualTo(expectedJobDataMap);
  }

  private static Stream<Arguments> provideImplementationClasses() {
    return Stream.of(
        Arguments.of(TypeEnum.TDRMANIFEST, TdrManifestQuartzJob.class),
        Arguments.of(TypeEnum.PFB, PfbQuartzJob.class));
  }

  // does createSchedulable use the correct implementation class for each import type?
  @ParameterizedTest(name = "for import type {0}, should use {1}")
  @MethodSource("provideImplementationClasses")
  void createSchedulableImplementationClasses(
      TypeEnum importType, Class<? extends Job> expectedClass) {
    UUID jobId = UUID.randomUUID();
    Schedulable actual = ImportService.createSchedulable(importType, jobId, Map.of());
    assertEquals(expectedClass, actual.getImplementation());
  }

  // this is the happy path for importService.createImport; we should end up with a
  // job in QUEUED status
  @ParameterizedTest(name = "for import type {0}")
  @EnumSource(
      value = TypeEnum.class,
      names = {"RAWLSJSON"},
      mode = Mode.EXCLUDE)
  void persistsJobAsQueued(TypeEnum importType) {
    // schedulerDao.schedule(), which returns void, returns successfully
    doNothing().when(schedulerDao).schedule(any(Schedulable.class));
    // define the import request
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);
    // perform the import request
    GenericJobServerModel createdJob =
        importService.createImport(defaultCollectionId().id(), importRequest);

    // re-retrieve the job; this double-checks what's actually in the db, in case the return
    // value of importService.createImport has bugs
    // this will also throw if the job was not persisted to the db
    GenericJobServerModel actual = jobDao.getJob(createdJob.getJobId());

    assertEquals(GenericJobServerModel.StatusEnum.QUEUED, actual.getStatus());
  }

  // importService.createImport should make appropriate calls to the SchedulerDao
  @ParameterizedTest(name = "for import type {0}")
  @EnumSource(
      value = TypeEnum.class,
      names = {"RAWLSJSON"},
      mode = Mode.EXCLUDE)
  void addsJobToScheduler(TypeEnum importType) {
    // schedulerDao.schedule(), which returns void, returns successfully
    doNothing().when(schedulerDao).schedule(any(Schedulable.class));
    // define the import request
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);
    // perform the import request
    GenericJobServerModel createdJob =
        importService.createImport(defaultCollectionId().id(), importRequest);
    // assert that importService.createImport properly calls schedulerDao
    verify(schedulerDao).schedule(schedulableCaptor.capture());
    Schedulable actual = schedulableCaptor.getValue();
    assertEquals(createdJob.getJobId().toString(), actual.getId(), "scheduled job had wrong id");
    assertEquals(importType.name(), actual.getGroup(), "scheduled job had wrong group");

    Map<String, Serializable> actualArguments = actual.getArguments();
    assertEquals(
        defaultCollectionId().toString(),
        actualArguments.get(ARG_COLLECTION),
        "scheduled job had wrong collection argument");
    assertEquals(
        importUri.toString(), actualArguments.get(ARG_URL), "scheduled job had wrong url argument");
    // The return value of mock sam's get pet token
    assertEquals("arbitraryToken", actualArguments.get(ARG_TOKEN), "scheduled job had wrong token");
  }

  // if we hit an error scheduling the job in Quartz, we should mark the job as being in ERROR
  @ParameterizedTest(name = "for import type {0}")
  @EnumSource(
      value = TypeEnum.class,
      names = {"RAWLSJSON"},
      mode = Mode.EXCLUDE)
  void failsJobIfSchedulingFails(TypeEnum importType) {
    // schedulerDao.schedule(), which returns void, returns successfully
    doThrow(new RuntimeException("unit test failme"))
        .when(schedulerDao)
        .schedule(any(Schedulable.class));
    // define the import request
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);
    // perform the import request; this will internally hit the exception from the schedulerDao
    GenericJobServerModel createdJob =
        importService.createImport(defaultCollectionId().id(), importRequest);

    // re-retrieve the job; this double-checks what's actually in the db, in case the return
    // value of importService.createImport has bugs
    // this will also throw if the job was not persisted to the db
    GenericJobServerModel actual = jobDao.getJob(createdJob.getJobId());

    assertEquals(GenericJobServerModel.StatusEnum.ERROR, actual.getStatus());
  }

  @ParameterizedTest(name = "for import type {0}")
  @EnumSource(
      value = TypeEnum.class,
      names = {"RAWLSJSON"},
      mode = Mode.EXCLUDE)
  void doesNotCreateJobWithoutPetToken(TypeEnum importType) throws ApiException {
    given(mockSamClientFactory.getGoogleApi(any(BearerToken.class))).willReturn(mockSamGoogleApi);
    // Sam permission check will always return true
    given(mockSamGoogleApi.getArbitraryPetServiceAccountToken(any()))
        .willThrow(new ApiException("token failure for unit test"));

    // schedulerDao.schedule(), which returns void, returns successfully
    doNothing().when(schedulerDao).schedule(any(Schedulable.class));

    // define the import request
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);
    // Import will fail without a pet token
    assertThrows(
        Exception.class,
        () -> importService.createImport(defaultCollectionId().id(), importRequest));
    // Job should not have been created
    verify(jobDao, times(0)).createJob(any());
  }

  @ParameterizedTest(name = "for import type {0}, validates import source before creating job")
  @EnumSource(
      value = TypeEnum.class,
      names = {"RAWLSJSON"},
      mode = Mode.EXCLUDE)
  void doesNotCreateJobIfImportSourceValidationFails(TypeEnum importType) {
    // Arrange
    // schedulerDao.schedule(), which returns void, returns successfully
    doNothing().when(schedulerDao).schedule(any(Schedulable.class));

    // Act/Assert
    URI testImportUri = URI.create("http://anvil.gi.ucsc.edu/testcontainer/file");
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(importType, testImportUri);
    ValidationException err =
        assertThrows(
            ValidationException.class,
            () -> importService.createImport(defaultCollectionId().id(), importRequest));

    // No import job should be created.
    verify(jobDao, never()).createJob(any());
  }

  @ParameterizedTest(name = "Options from import request should be passed through to job {0}")
  @ValueSource(booleans = {true, false})
  void passesThroughIsUpsert(boolean syncPermissions) {
    // Arrange
    TestUtils.createCollection(collectionService, workspaceId);
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(TypeEnum.TDRMANIFEST, importUri);
    importRequest.getOptions().put(OPTION_TDR_SYNC_PERMISSIONS, syncPermissions);

    // Act
    importService.createImport(defaultCollectionId().id(), importRequest);

    // Assert
    verify(schedulerDao).schedule(schedulableCaptor.capture());
    Map<String, Serializable> actualArguments = schedulableCaptor.getValue().getArguments();

    ImportJobInput importJobInput = (ImportJobInput) actualArguments.get(ARG_IMPORT_JOB_INPUT);
    TdrManifestImportOptions options = (TdrManifestImportOptions) importJobInput.getOptions();
    assertEquals(syncPermissions, options.syncPermissions());
  }

  private CollectionId defaultCollectionId() {
    return CollectionId.of(workspaceId.id());
  }
}
