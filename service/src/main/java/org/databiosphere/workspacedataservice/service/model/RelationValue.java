package org.databiosphere.workspacedataservice.service.model;

import org.databiosphere.workspacedataservice.shared.model.Record;

public record RelationValue(Record fromRecord, Record toRecord) {}
