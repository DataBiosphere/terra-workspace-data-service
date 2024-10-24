package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CloneResponse(UUID sourceWorkspaceId, CloneStatus status) implements JobResult {}
