package org.databiosphere.workspacedataservice.activitylog;

public record ActivityEvent(String subject,
                            ActivityModels.Action action,
                            ActivityModels.Thing thing,
                            String[] ids) {}
