package org.databiosphere.workspacedataservice.metrics;

import io.micrometer.core.instrument.DistributionSummary;

/** Wrapper for distribution summary of number of snapshots within a PFB or other import source */
public record SnapshotsConsideredDistributionSummary(DistributionSummary distributionSummary) {}
