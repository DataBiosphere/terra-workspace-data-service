package org.databiosphere.workspacedataservice.metrics;

import io.micrometer.core.instrument.DistributionSummary;

/** Wrapper for distribution summary of number of upserts */
public record RecordUpsertDistributionSummary(DistributionSummary distributionSummary) {}
