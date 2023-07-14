package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;

import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record BackupRestoreResponse(String filename, UUID requester, String description) implements JobResult {

}
