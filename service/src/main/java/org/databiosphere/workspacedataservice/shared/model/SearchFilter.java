package org.databiosphere.workspacedataservice.shared.model;

import java.util.List;
import org.springframework.lang.Nullable;

public record SearchFilter(@Nullable List<String> ids) {}
