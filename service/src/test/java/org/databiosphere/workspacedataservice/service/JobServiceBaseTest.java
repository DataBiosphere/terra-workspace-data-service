package org.databiosphere.workspacedataservice.service;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;

public abstract class JobServiceBaseTest {

  List<String> allStatuses =
      Arrays.stream(GenericJobServerModel.StatusEnum.values())
          .map(GenericJobServerModel.StatusEnum::toString)
          .toList();

  GenericJobServerModel makeJob(UUID jobId, CollectionId collectionId) {
    return new GenericJobServerModel(
        jobId,
        GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
        collectionId.id(),
        GenericJobServerModel.StatusEnum.RUNNING,
        // set created and updated to now, but in UTC because that's how Postgres stores it
        OffsetDateTime.now(ZoneId.of("Z")),
        OffsetDateTime.now(ZoneId.of("Z")));
  }

  List<GenericJobServerModel> makeJobList(CollectionId collectionId, int count) {
    return IntStream.range(0, count)
        .mapToObj(idx -> makeJob(UUID.randomUUID(), collectionId))
        .toList();
  }
}
