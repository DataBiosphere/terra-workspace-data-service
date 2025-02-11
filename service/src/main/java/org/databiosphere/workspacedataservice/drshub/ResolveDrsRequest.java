package org.databiosphere.workspacedataservice.drshub;

import java.util.List;

public record ResolveDrsRequest(String url, List<String> fields) {}
