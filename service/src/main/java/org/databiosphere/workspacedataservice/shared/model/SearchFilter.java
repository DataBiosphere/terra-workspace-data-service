package org.databiosphere.workspacedataservice.shared.model;

import java.util.List;
import java.util.Optional;

public record SearchFilter(Optional<List<String>> ids) {}
