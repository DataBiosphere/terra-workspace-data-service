package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_INSTANCE;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.SchedulerDao;
import org.databiosphere.workspacedataservice.dataimport.PfbQuartzJob;
import org.databiosphere.workspacedataservice.dataimport.TdrManifestQuartzJob;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.Schedulable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"mock-sam", "mock-instance-dao"})
@SpringBootTest
class ImportServiceTest {

  @Autowired ImportService importService;
  @Autowired InstanceService instanceService;
  @Autowired JobDao jobDao;
  @MockBean SchedulerDao schedulerDao;

  private static final String VERSION = "v0.2";

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

  // does createSchedulable use the correct implementation class for TDRMANIFEST?
  @ParameterizedTest(name = "for import type {0}, should use {1}")
  @MethodSource("provideImplementationClasses")
  void createSchedulableImplementationClasses(
      TypeEnum importType, Class<? extends Job> expectedClass) {
    UUID jobId = UUID.randomUUID();
    Schedulable actual = importService.createSchedulable(importType, jobId, Map.of());
    assertEquals(expectedClass, actual.getImplementation());
  }

  // this is the happy path for importService.createImport
  @ParameterizedTest(name = "for import type {0}")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void persistsJobAsQueued(ImportRequestServerModel.TypeEnum importType) {
    // schedulerDao.schedule(), which returns void, returns successfully
    doNothing().when(schedulerDao).schedule(any(Schedulable.class));
    // create instance (in the MockInstanceDao)
    UUID instanceId = UUID.randomUUID();
    instanceService.createInstance(instanceId, VERSION);
    // define the import request
    URI importUri = URI.create("http://does/not/matter");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);
    // perform the import request
    GenericJobServerModel createdJob = importService.createImport(instanceId, importRequest);

    // re-retrieve the job; this double-checks what's actually in the db, in case the return
    // value of importService.createImport has bugs
    // this will also throw if the job was not persisted to the db
    GenericJobServerModel actual = jobDao.getJob(createdJob.getJobId());

    assertEquals(GenericJobServerModel.StatusEnum.QUEUED, actual.getStatus());
  }

  // this is the happy path for importService.createImport
  @ParameterizedTest(name = "for import type {0}")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void addsJobToScheduler(ImportRequestServerModel.TypeEnum importType) {
    // schedulerDao.schedule(), which returns void, returns successfully
    doNothing().when(schedulerDao).schedule(any(Schedulable.class));
    // create instance (in the MockInstanceDao)
    UUID instanceId = UUID.randomUUID();
    instanceService.createInstance(instanceId, VERSION);
    // define the import request
    URI importUri = URI.create("http://does/not/matter");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);
    // perform the import request
    GenericJobServerModel createdJob = importService.createImport(instanceId, importRequest);
    // assert that importService.createImport properly calls schedulerDao
    ArgumentCaptor<Schedulable> argument = ArgumentCaptor.forClass(Schedulable.class);
    verify(schedulerDao).schedule(argument.capture());
    Schedulable actual = argument.getValue();
    assertEquals(createdJob.getJobId().toString(), actual.getId(), "scheduled job had wrong id");
    assertEquals(importType.name(), actual.getGroup(), "scheduled job had wrong group");

    Map<String, Serializable> actualArguments = actual.getArguments();
    assertEquals(
        instanceId.toString(),
        actualArguments.get(ARG_INSTANCE),
        "scheduled job had wrong instance argument");
    assertEquals(
        importUri.toString(), actualArguments.get(ARG_URL), "scheduled job had wrong url argument");
    // TODO: add an assertion for the pet token in the arguments, once we have pet tokens hooked up
  }

  // TODO: this test is failing; we need more error-trapping in ImportService
  @ParameterizedTest(name = "for import type {0}")
  @EnumSource(ImportRequestServerModel.TypeEnum.class)
  void failsJobIfSchedulingFails(ImportRequestServerModel.TypeEnum importType) {
    // schedulerDao.schedule(), which returns void, returns successfully
    doThrow(new RuntimeException("unit test failme"))
        .when(schedulerDao)
        .schedule(any(Schedulable.class));
    // create instance (in the MockInstanceDao)
    UUID instanceId = UUID.randomUUID();
    instanceService.createInstance(instanceId, VERSION);
    // define the import request
    URI importUri = URI.create("http://does/not/matter");
    ImportRequestServerModel importRequest = new ImportRequestServerModel(importType, importUri);
    // perform the import request; this will internally hit the exception from the schedulerDao
    GenericJobServerModel createdJob = importService.createImport(instanceId, importRequest);

    // re-retrieve the job; this double-checks what's actually in the db, in case the return
    // value of importService.createImport has bugs
    // this will also throw if the job was not persisted to the db
    GenericJobServerModel actual = jobDao.getJob(createdJob.getJobId());

    assertEquals(GenericJobServerModel.StatusEnum.ERROR, actual.getStatus());
  }
}
