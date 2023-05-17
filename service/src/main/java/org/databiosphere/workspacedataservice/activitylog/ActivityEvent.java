package org.databiosphere.workspacedataservice.activitylog;

import org.databiosphere.workspacedataservice.shared.model.RecordType;

public record ActivityEvent(String subject,
                            ActivityModels.Action action,
                            ActivityModels.Thing thing,
                            RecordType recordType,
                            Integer quantity,
                            String[] ids) {}
