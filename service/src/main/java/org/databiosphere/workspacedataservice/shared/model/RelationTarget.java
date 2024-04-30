package org.databiosphere.workspacedataservice.shared.model;

/**
 * A relation from one WDS record to another.
 *
 * @param targetType
 * @param targetId
 */
public record RelationTarget(RecordType targetType, String targetId) {}
