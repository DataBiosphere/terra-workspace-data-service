package org.databiosphere.workspacedataservice.metrics;

import io.micrometer.core.instrument.DistributionSummary;

/** Wrapper for distribution summary of number of snapshots actually linked by an import job */
public record SnapshotsLinkedDistributionSummary(DistributionSummary distributionSummary) {}
