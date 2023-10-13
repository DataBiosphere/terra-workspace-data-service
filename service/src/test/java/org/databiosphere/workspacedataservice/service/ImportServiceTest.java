package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.dataimport.PfbQuartzJob;
import org.databiosphere.workspacedataservice.dataimport.TdrManifestQuartzJob;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.Schedulable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ImportServiceTest {

  @Autowired ImportService importService;

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

    assertEquals(jobId.toString(), actual.getName());
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

  // TODO: add test coverage
  //    - writes a row to sys_wds.job as CREATED
  //    - with Quartz mock, schedules a job
  //    - saves token, url, and instance to the scheduled job
  //    - updates row in sys_wds.job to QUEUED
}
