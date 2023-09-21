package org.databiosphere.workspacedataservice.activitylog;

import org.databiosphere.workspacedataservice.shared.model.RecordType;

/**
 * Represents an entry in an activity log.
 *
 * @param subject the identity performing the activity
 * @param action the "verb" of the activity
 * @param thing the object target of the activity
 * @param recordType further qualifies the object, if this is a table or record
 * @param quantity number of things being operated on, if "ids" is not specified
 * @param ids ids for the things being operated on
 */
public record ActivityEvent(
    String subject,
    ActivityModels.Action action,
    ActivityModels.Thing thing,
    RecordType recordType,
    Integer quantity,
    String[] ids) {}
