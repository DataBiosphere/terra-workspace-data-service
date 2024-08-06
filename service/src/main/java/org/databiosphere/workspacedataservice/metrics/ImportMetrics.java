package org.databiosphere.workspacedataservice.metrics;

import org.springframework.stereotype.Component;

/**
 * A collection of metrics definitions to be passed into and used by import jobs.
 *
 * @param recordUpsertDistributionSummary distribution summary of number of record updates per
 *     import
 */
@Component
public record ImportMetrics(
    RecordUpsertDistributionSummary recordUpsertDistributionSummary,
    SnapshotsConsideredDistributionSummary snapshotsConsideredDistributionSummary,
    SnapshotsLinkedDistributionSummary snapshotsLinkedDistributionSummary) {}
