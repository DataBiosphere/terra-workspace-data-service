package org.databiosphere.workspacedataservice.shared.model.job;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class JobStatusTest {

  // are the hand-coded JobStatus enum and the generated GenericJobServerModel.StatusEnum the same?
  @Test
  public void equivalentToGeneratedModel() {
    // String version of the JobStatus enum values
    List<String> names = Arrays.stream(JobStatus.values()).map(Enum::name).toList();

    // String version of the StatusEnum enum values
    List<String> generatedNames =
        Arrays.stream(GenericJobServerModel.StatusEnum.values()).map(Enum::name).toList();

    // should be the same!
    assertEquals(generatedNames, names);
  }

  private static Stream<Arguments> provideJobStatuses() {
    return Arrays.stream(JobStatus.values()).map(Arguments::of);
  }

  @ParameterizedTest(name = "JobStatus {0} should translate to GenericJobServerModel.StatusEnum")
  @MethodSource("provideJobStatuses")
  public void jobStatusToGenerated(JobStatus jobStatus) {
    GenericJobServerModel.StatusEnum actual = jobStatus.toGeneratedModel();
    assertEquals(jobStatus.name(), actual.name());
  }

  private static Stream<Arguments> provideStatusEnums() {
    return Arrays.stream(GenericJobServerModel.StatusEnum.values()).map(Arguments::of);
  }

  @ParameterizedTest(name = "GenericJobServerModel.StatusEnum {0} should translate to JobStatus")
  @MethodSource("provideStatusEnums")
  public void jobStatusFromGenerated(GenericJobServerModel.StatusEnum statusEnum) {
    JobStatus actual = JobStatus.fromGeneratedModel(statusEnum);
    assertEquals(statusEnum.name(), actual.name());
  }
}
