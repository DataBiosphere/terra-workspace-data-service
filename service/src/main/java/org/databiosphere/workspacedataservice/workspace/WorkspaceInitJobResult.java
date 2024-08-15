package org.databiosphere.workspacedataservice.workspace;

import java.io.Serializable;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;

/**
 * JobResult implementation for workspace initialization. TODO AJ-1952 should we add anything here
 * to represent cloning details?
 *
 * @param defaultCollectionCreated
 * @param isClone
 */
public record WorkspaceInitJobResult(boolean defaultCollectionCreated, boolean isClone)
    implements JobResult, Serializable {}
