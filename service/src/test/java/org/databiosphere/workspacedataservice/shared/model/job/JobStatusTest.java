package org.databiosphere.workspacedataservice.shared.model.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class JobStatusTest {

  // are the hand-coded JobStatus enum and the generated GenericJobServerModel.StatusEnum the same?
  @Test
  void equivalentToGeneratedModel() {
    // String version of the JobStatus enum values
    List<String> names = Arrays.stream(JobStatus.values()).map(Enum::name).toList();

    // String version of the StatusEnum enum values
    List<String> generatedNames =
        Arrays.stream(GenericJobServerModel.StatusEnum.values()).map(Enum::name).toList();

    // should be the same!
    assertEquals(generatedNames, names);
  }

  @ParameterizedTest(name = "JobStatus {0} should translate to GenericJobServerModel.StatusEnum")
  @EnumSource(JobStatus.class)
  void jobStatusToGenerated(JobStatus jobStatus) {
    GenericJobServerModel.StatusEnum actual = jobStatus.toGeneratedModel();
    assertEquals(jobStatus.name(), actual.name());
  }

  @ParameterizedTest(name = "GenericJobServerModel.StatusEnum {0} should translate to JobStatus")
  @EnumSource(GenericJobServerModel.StatusEnum.class)
  void jobStatusFromGenerated(GenericJobServerModel.StatusEnum statusEnum) {
    JobStatus actual = JobStatus.fromGeneratedModel(statusEnum);
    assertEquals(statusEnum.name(), actual.name());
  }
}
