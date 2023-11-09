package org.databiosphere.workspacedataservice.dataimport;

import static org.databiosphere.workspacedataservice.dataimport.PfbRecordConverter.ID_FIELD;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_INSTANCE;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.pfb.PfbReader;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.service.model.exception.PfbParsingException;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
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
  private final BatchWriteService batchWriteService;
  private final ActivityLogger activityLogger;
  private final UUID workspaceId;
  private final RestClientRetry restClientRetry;

  public PfbQuartzJob(
      JobDao jobDao,
      WorkspaceManagerDao wsmDao,
      RestClientRetry restClientRetry,
      BatchWriteService batchWriteService,
      ActivityLogger activityLogger,
      @Value("${twds.instance.workspace-id}") UUID workspaceId) {
    this.jobDao = jobDao;
    this.wsmDao = wsmDao;
    this.restClientRetry = restClientRetry;
    this.workspaceId = workspaceId;
    this.batchWriteService = batchWriteService;
    this.activityLogger = activityLogger;
  }

  @Override
  protected JobDao getJobDao() {
    return this.jobDao;
  }

  @Override
  protected void executeInternal(UUID jobId, JobExecutionContext context) {
    // Grab the PFB url from the job's data map
    JobDataMap jobDataMap = context.getMergedJobDataMap();
    URL url = getJobDataUrl(jobDataMap, ARG_URL);
    UUID targetInstance = getJobDataUUID(jobDataMap, ARG_INSTANCE);

    // Find all the snapshot ids in the PFB, then create or verify references from the
    // workspace to the snapshot for each of those snapshot ids.
    // This will throw an exception if there are policy conflicts between the workspace
    // and the snapshots.
    // TODO AJ-1452: can this pass also identify all schemas/datatypes?
    // TODO AJ-1452: can this pass also check if record types are contiguous?
    logger.info("Finding snapshots in this PFB...");
    List<UUID> snapshotIds = withPfbStream(url, this::findSnapshots);

    logger.info("Linking snapshots...");
    linkSnapshots(snapshotIds);

    // Import all the tables and rows inside the PFB.
    logger.info("Importing tables and rows from this PFB...");
    withPfbStream(url, stream -> importTables(stream, targetInstance));

    // TODO AJ-1453: save the result of importTables and persist the to the job
  }

  /**
   * definition for some function that consumes a PFB stream (as a DataFileStream<GenericRecord>)
   */
  @FunctionalInterface
  public interface PfbStreamConsumer<T> {
    T run(DataFileStream<GenericRecord> dataStream);
  }

  /**
   * convenience wrapper function to execute a PfbStreamConsumer on a PFB at a given url, handling
   * opening and closing of a DataFileStream for that PFB.
   *
   * @param url location of the PFB
   * @param consumer code to execute against the PFB's contents
   */
  <T> T withPfbStream(URL url, PfbStreamConsumer<T> consumer) {
    try (DataFileStream<GenericRecord> dataStream =
        PfbReader.getGenericRecordsStream(url.toString())) {
      return consumer.run(dataStream);
    } catch (Exception e) {
      throw new PfbParsingException("Error processing PFB", e);
    }
  }

  /**
   * Given a DataFileStream representing a PFB, import all the tables and rows inside that PFB.
   *
   * @param dataStream stream representing the PFB.
   */
  BatchWriteResult importTables(DataFileStream<GenericRecord> dataStream, UUID targetInstance) {
    BatchWriteResult result =
        batchWriteService.batchWritePfbStream(dataStream, targetInstance, Optional.of(ID_FIELD));

    result
        .entrySet()
        .forEach(
            entry -> {
              RecordType recordType = entry.getKey();
              int quantity = entry.getValue();
              activityLogger.saveEventForCurrentUser(
                  user -> user.upserted().record().withRecordType(recordType).ofQuantity(quantity));
            });
    return result;
  }

  /**
   * Given a DataFileStream representing a PFB, find all the unique snapshot ids in the PFB by
   * looking in the "source_datarepo_snapshot_id" column of each row in the PFB
   *
   * @param dataStream stream representing the PFB.
   * @return unique UUIDs found in the PFB
   */
  List<UUID> findSnapshots(DataFileStream<GenericRecord> dataStream) {
    // translate the Avro DataFileStream into a Java stream
    Stream<GenericRecord> recordStream =
        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(dataStream.iterator(), Spliterator.ORDERED), false);

    // process the stream into a list of unique snapshotIds
    return recordStream
        .map(rec -> rec.get("object")) // Records in a pfb are stored under the key "object"
        .filter(GenericRecord.class::isInstance) // which we expect to be a GenericRecord
        .map(GenericRecord.class::cast)
        .filter(obj -> obj.hasField(SNAPSHOT_ID_IDENTIFIER)) // avoid exception if field nonexistent
        .map(obj -> obj.get(SNAPSHOT_ID_IDENTIFIER)) // within the GenericRecord, find the
        // source_datarepo_snapshot_id
        .filter(Objects::nonNull) // expect source_datarepo_snapshot_id to be non-null
        .map(obj -> maybeUuid(obj.toString()))
        .filter(Objects::nonNull)
        .distinct() // find only the unique snapshotids
        .toList();
  }

  /**
   * Given a list of snapshot ids, create references from the workspace to the snapshot for each id
   * that does not already have a reference.
   *
   * @param snapshotIds the list of snapshot ids to create or verify references.
   */
  protected void linkSnapshots(List<UUID> snapshotIds) {
    // list existing snapshots linked to this workspace
    TdrSnapshotSupport tdrSnapshotSupport =
        new TdrSnapshotSupport(workspaceId, wsmDao, restClientRetry);
    List<UUID> existingSnapshotIds =
        tdrSnapshotSupport.existingPolicySnapshotIds(/* pageSize= */ 50);
    // find the snapshots in this PFB that are not already linked to this workspace
    List<UUID> newSnapshotIds =
        snapshotIds.stream().filter(id -> !existingSnapshotIds.contains(id)).toList();

    logger.info(
        "PFB contains {} snapshot ids. {} of these are already linked to the workspace; {} new links will be created.",
        snapshotIds.size(),
        snapshotIds.size() - newSnapshotIds.size(),
        newSnapshotIds.size());

    // pass snapshotIds to WSM
    for (UUID uuid : newSnapshotIds) {
      try {
        RestClientRetry.VoidRestCall voidRestCall =
            (() -> wsmDao.linkSnapshotForPolicy(new SnapshotModel().id(uuid)));
        restClientRetry.withRetryAndErrorHandling(
            voidRestCall, "WSM.createDataRepoSnapshotReference");
      } catch (Exception e) {
        throw new PfbParsingException("Error processing PFB: " + e.getMessage(), e);
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
