package org.databiosphere.workspacedataservice.dataimport;

import static org.databiosphere.workspacedataservice.service.ImportService.ARG_IMPORT_JOB_INPUT;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_COLLECTION;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.function.Supplier;
import org.databiosphere.workspacedataservice.jobexec.JobDataMapReader;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.stereotype.Component;

/** Retrieves various common details needed for import jobs. */
@Component
public class ImportDetailsRetriever {

  private final SamDao samDao;
  private final CollectionService collectionService;
  private final ObjectMapper objectMapper;

  ImportDetailsRetriever(
      SamDao samDao, CollectionService collectionService, ObjectMapper objectMapper) {
    this.samDao = samDao;
    this.collectionService = collectionService;
    this.objectMapper = objectMapper;
  }

  public ImportDetails fetch(UUID jobId, JobDataMapReader jobData, PrefixStrategy prefixStrategy) {
    // Collect details needed for import
    UUID targetCollection = jobData.getUUID(ARG_COLLECTION);
    String authToken = jobData.getString(ARG_TOKEN);
    Supplier<String> userEmailSupplier = () -> samDao.getUserEmail(BearerToken.of(authToken));

    // determine the workspace for the target collection
    CollectionId collectionId = CollectionId.of(targetCollection);
    WorkspaceId workspaceId = collectionService.getWorkspaceId(collectionId);

    ImportJobInput importJobInput;
    try {
      importJobInput =
          objectMapper.readValue(jobData.getString(ARG_IMPORT_JOB_INPUT), ImportJobInput.class);
    } catch (JsonProcessingException e) {
      // This should not happen, since import options was serialized by ImportService.
      throw new RuntimeException("Error deserializing import options", e);
    }

    return new ImportDetails(
        jobId, userEmailSupplier, workspaceId, collectionId, prefixStrategy, importJobInput);
  }
}
