package org.databiosphere.workspacedataservice.dataimport;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.pfb.PfbReader;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.model.exception.PfbParsingException;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Shell/starting point for PFB import via Quartz. */
@Component
public class PfbQuartzJob extends QuartzJob {

  public static final String SNAPSHOT_ID_IDENTIFIER = "source_datarepo_snapshot_id";

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final JobDao jobDao;
  private final WorkspaceManagerDao wsmDao;

  @Value("${twds.instance.workspace-id}")
  UUID workspaceId;

  private final RestClientRetry restClientRetry;

  public PfbQuartzJob(JobDao jobDao, WorkspaceManagerDao wsmDao, RestClientRetry restClientRetry) {
    this.jobDao = jobDao;
    this.wsmDao = wsmDao;
    this.restClientRetry = restClientRetry;
  }

  @Override
  protected JobDao getJobDao() {
    return this.jobDao;
  }

  @Override
  protected void executeInternal(UUID jobId, JobExecutionContext context) {
    JobDataMap jobDataMap = context.getMergedJobDataMap();
    URL url = getJobDataUrl(jobDataMap, ARG_URL);
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      // translate the Avro DataFileStream into a Java stream
      Stream<GenericRecord> recordStream =
          StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(dataStream.iterator(), Spliterator.ORDERED),
              false);

      // process the stream into a list of unique snapshotIds
      List<UUID> snapshotIds =
          recordStream
              .map(rec -> rec.get("object")) // Records in a pfb are stored under the key "object"
              .filter(GenericRecord.class::isInstance) // which we expect to be a GenericRecord
              .map(GenericRecord.class::cast)
              .filter(
                  obj ->
                      obj.hasField(SNAPSHOT_ID_IDENTIFIER)) // avoid exception if field nonexistent
              .map(obj -> obj.get(SNAPSHOT_ID_IDENTIFIER)) // within the GenericRecord, find the
              // source_datarepo_snapshot_id
              .filter(Objects::nonNull) // expect source_datarepo_snapshot_id to be non-null
              .map(obj -> maybeUuid(obj.toString()))
              .filter(Objects::nonNull)
              .distinct() // find only the unique snapshotids
              .toList();

      // link the found snapshots to the workspace, skipping any that were previously linked
      linkSnapshots(snapshotIds);

    } catch (IOException e) {
      throw new PfbParsingException("Error processing PFB", e);
    }

    // TODO: AJ-1227 implement PFB import.
    logger.info("TODO: implement PFB import.");
  }

  protected void linkSnapshots(List<UUID> snapshotIds) {
    // list existing snapshots linked to this workspace
    List<UUID> existingSnapshotIds =
        PfbQuartzJobSupport.existingPolicySnapshotIds(workspaceId, 50, wsmDao, restClientRetry);
    // find the snapshots in this PFB that are not already linked to this workspace
    List<UUID> newSnapshotIds =
        snapshotIds.stream().filter(id -> !existingSnapshotIds.contains(id)).toList();

    logger.info(
        "PFB contains {} snapshot ids. {} of these are already linked to the workspace; {} new links will be created.",
        snapshotIds.size(),
        snapshotIds.size() - newSnapshotIds.size(),
        snapshotIds.size());

    // pass snapshotIds to WSM
    for (UUID uuid : newSnapshotIds) {
      try {
        RestClientRetry.VoidRestCall voidRestCall =
            (() -> wsmDao.createDataRepoSnapshotReference(new SnapshotModel().id(uuid)));
        restClientRetry.withRetryAndErrorHandling(
            voidRestCall, "WSM.createDataRepoSnapshotReference");
      } catch (Exception e) {
        throw new PfbParsingException("Error processing PFB: Invalid snapshot UUID", e);
      }
    }
  }

  private UUID maybeUuid(String input) {
    try {
      return UUID.fromString(input);
    } catch (Exception e) {
      logger.warn("found unparseable snapshot id '{}' in PFB contents", input);
      return null;
    }
  }
}
