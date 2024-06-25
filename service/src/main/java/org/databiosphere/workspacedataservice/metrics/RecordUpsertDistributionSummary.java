package org.databiosphere.workspacedataservice.metrics;

import io.micrometer.core.instrument.DistributionSummary;

public record RecordUpsertDistributionSummary(DistributionSummary distributionSummary) {}
