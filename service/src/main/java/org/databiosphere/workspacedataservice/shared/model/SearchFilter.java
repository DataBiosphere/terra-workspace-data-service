package org.databiosphere.workspacedataservice.shared.model;

import java.util.List;
import java.util.Optional;
import org.databiosphere.workspacedata.model.FilterColumn;

public record SearchFilter(Optional<List<String>> ids, Optional<List<FilterColumn>> filters) {}
