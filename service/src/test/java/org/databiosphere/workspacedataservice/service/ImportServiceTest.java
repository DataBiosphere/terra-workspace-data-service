package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_COLLECTION;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.MockCollectionDao;
import org.databiosphere.workspacedataservice.dao.SchedulerDao;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbQuartzJob;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestQuartzJob;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.Schedulable;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"mock-sam", "mock-collection-dao"})
@DirtiesContext
@SpringBootTest
class ImportServiceTest extends TestBase {

  @Autowired ImportService importService;
  @Autowired CollectionDao collectionDao;
  @Autowired CollectionService collectionService;
  @Autowired @SingleTenant WorkspaceId workspaceId;
  @SpyBean JobDao jobDao;
  @MockBean SchedulerDao schedulerDao;
  @MockBean SamClientFactory mockSamClientFactory;

  ResourcesApi mockSamResourcesApi = Mockito.mock(ResourcesApi.class);
  GoogleApi mockSamGoogleApi = Mockito.mock(GoogleApi.class);

  private final URI importUri =
      URI.create("https://teststorageaccount.blob.core.windows.net/testcontainer/path/to/file");

  private static final String VERSION = "v0.2";

  @BeforeEach
  void setUp() throws ApiException {
    // return the mock ResourcesApi from the mock SamClientFactory
    given(mockSamClientFactory.getResourcesApi()).willReturn(mockSamResourcesApi);
    // Sam permission check will always return true
    given(mockSamResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
        .willReturn(true);
    given(mockSamClientFactory.getGoogleApi(any(BearerToken.class))).willReturn(mockSamGoogleApi);
    // Pet token request returns "arbitraryToken"
    given(mockSamGoogleApi.getArbitraryPetServiceAccountToken(any())).willReturn("arbitraryToken");

    // reset to zero collections
    if (collectionDao instanceof MockCollectionDao mockCollectionDao) {
      mockCollectionDao.clearAllCollections();
    }

    // clear call history for the mock
    Mockito.clearInvocations(mockSamResourcesApi);
  }

  // does createSchedulable properly store the jobId, job group, and job data map?
  @ParameterizedTest(name = "for import type {0}")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void createSchedulableValues(ImportRequestServerModel.TypeEnum importType) {
    UUID jobId = UUID.randomUUID();
    Map<String, Serializable> arguments = Map.of("foo", "bar", "twenty-three", 23);

    Schedulable actual = importService.createSchedulable(importType, jobId, arguments);

    JobDataMap expectedJobDataMap = new JobDataMap();
    expectedJobDataMap.put("foo", "bar");
    expectedJobDataMap.put("twenty-three", 23);

    assertEquals(jobId.toString(), actual.getId());
    assertEquals(importType.name(), actual.getGroup());
    assertEquals(expectedJobDataMap, actual.getArgumentsAsJobDataMap());
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
    Schedulable actual = importService.createSchedulable(importType, jobId, Map.of());
    assertEquals(expectedClass, actual.getImplementation());
  }

  // this is the happy path for importService.createImport; we should end up with a
  // job in QUEUED status
  @ParameterizedTest(name = "for import type {0}")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void persistsJobAsQueued(ImportRequestServerModel.TypeEnum importType) {
    // schedulerDao.schedule(), which returns void, returns successfully
    doNothing().when(schedulerDao).schedule(any(Schedulable.class));
    // create collection (in the MockCollectionDao)
    collectionService.createCollection(workspaceId, defaultCollectionId(), VERSION);
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
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void addsJobToScheduler(ImportRequestServerModel.TypeEnum importType) {
    // schedulerDao.schedule(), which returns void, returns successfully
    doNothing().when(schedulerDao).schedule(any(Schedulable.class));
    // create collection (in the MockCollectionDao)
    collectionService.createCollection(workspaceId, defaultCollectionId(), VERSION);
    // define the import request
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);
    // perform the import request
    GenericJobServerModel createdJob =
        importService.createImport(defaultCollectionId().id(), importRequest);
    // assert that importService.createImport properly calls schedulerDao
    ArgumentCaptor<Schedulable> argument = ArgumentCaptor.forClass(Schedulable.class);
    verify(schedulerDao).schedule(argument.capture());
    Schedulable actual = argument.getValue();
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
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void failsJobIfSchedulingFails(ImportRequestServerModel.TypeEnum importType) {
    // schedulerDao.schedule(), which returns void, returns successfully
    doThrow(new RuntimeException("unit test failme"))
        .when(schedulerDao)
        .schedule(any(Schedulable.class));
    // create collection (in the MockCollectionDao)
    collectionService.createCollection(workspaceId, defaultCollectionId(), VERSION);
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
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void doesNotCreateJobWithoutPetToken(ImportRequestServerModel.TypeEnum importType)
      throws ApiException {
    given(mockSamClientFactory.getGoogleApi(any(BearerToken.class))).willReturn(mockSamGoogleApi);
    // Sam permission check will always return true
    given(mockSamGoogleApi.getArbitraryPetServiceAccountToken(any()))
        .willThrow(new ApiException("token failure for unit test"));

    // schedulerDao.schedule(), which returns void, returns successfully
    doNothing().when(schedulerDao).schedule(any(Schedulable.class));
    // create collection (in the MockCollectionDao)
    UUID randomCollectionId = UUID.randomUUID();
    collectionService.createCollection(workspaceId, CollectionId.of(randomCollectionId), VERSION);
    // define the import request

    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);
    // Import will fail without a pet token
    assertThrows(
        Exception.class, () -> importService.createImport(randomCollectionId, importRequest));
    // Job should not have been created
    verify(jobDao, times(0)).createJob(any());
  }

  @ParameterizedTest(name = "for import type {0}, validates import source before creating job")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void doesNotCreateJobIfImportSourceValidationFails(ImportRequestServerModel.TypeEnum importType) {
    // Arrange
    // schedulerDao.schedule(), which returns void, returns successfully
    doNothing().when(schedulerDao).schedule(any(Schedulable.class));
    // create collection (in the MockCollectionDao)
    collectionService.createCollection(workspaceId, defaultCollectionId(), VERSION);

    // Act/Assert
    URI importUri =
        URI.create("http://teststorageaccount.blob.core.windows.net/testcontainer/file");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);
    ValidationException err =
        assertThrows(
            ValidationException.class,
            () -> importService.createImport(defaultCollectionId().id(), importRequest));

    // No import job should be created.
    verify(jobDao, never()).createJob(any());
  }

  private CollectionId defaultCollectionId() {
    return CollectionId.of(workspaceId.id());
  }
}
