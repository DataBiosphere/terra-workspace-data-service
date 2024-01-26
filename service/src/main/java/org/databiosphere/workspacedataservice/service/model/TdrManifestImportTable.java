package org.databiosphere.workspacedataservice.service.model;

import bio.terra.datarepo.model.RelationshipModel;
import java.net.URL;
import java.util.List;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

public record TdrManifestImportTable(
    RecordType recordType,
    String primaryKey,
    List<URL> dataFiles,
    List<RelationshipModel> relations) {}
