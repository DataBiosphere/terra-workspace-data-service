package org.databiosphere.workspacedataservice.dataimport.pfb;

import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbRecordConverter.ID_FIELD;
import static org.databiosphere.workspacedataservice.recordsource.RecordSource.ImportMode.BASE_ATTRIBUTES;
import static org.databiosphere.workspacedataservice.recordsource.RecordSource.ImportMode.RELATIONS;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_COLLECTION;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import bio.terra.pfb.PfbReader;
import io.micrometer.observation.ObservationRegistry;
import java.net.URL;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.SnapshotSupport;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.SnapshotSupportFactory;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;
import org.databiosphere.workspacedataservice.recordsink.RecordSink;
import org.databiosphere.workspacedataservice.recordsink.RecordSinkFactory;
import org.databiosphere.workspacedataservice.recordsource.RecordSource.ImportMode;
import org.databiosphere.workspacedataservice.recordsource.RecordSourceFactory;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.service.model.exception.PfbImportException;
import org.databiosphere.workspacedataservice.service.model.exception.PfbParsingException;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/** Shell/starting point for PFB import via Quartz. */
@Component
public class PfbQuartzJob extends QuartzJob {

  public static final String SNAPSHOT_ID_IDENTIFIER = "source_datarepo_snapshot_id";

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final JobDao jobDao;
  private final BatchWriteService batchWriteService;
  private final CollectionService collectionService;
  private final ActivityLogger activityLogger;
  private final RecordSourceFactory recordSourceFactory;
  private final RecordSinkFactory recordSinkFactory;
  private final SnapshotSupportFactory snapshotSupportFactory;
  private final SamDao samDao;

  public PfbQuartzJob(
      JobDao jobDao,
      RecordSourceFactory recordSourceFactory,
      RecordSinkFactory recordSinkFactory,
      BatchWriteService batchWriteService,
      CollectionService collectionService,
      ActivityLogger activityLogger,
      SamDao samDao,
      ObservationRegistry observationRegistry,
      SnapshotSupportFactory snapshotSupportFactory,
      DataImportProperties dataImportProperties) {
    super(observationRegistry, dataImportProperties);
    this.jobDao = jobDao;
    this.recordSourceFactory = recordSourceFactory;
    this.recordSinkFactory = recordSinkFactory;
    this.batchWriteService = batchWriteService;
    this.collectionService = collectionService;
    this.activityLogger = activityLogger;
    this.samDao = samDao;
    this.snapshotSupportFactory = snapshotSupportFactory;
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

    // Collect details needed for import
    UUID targetCollection = getJobDataUUID(jobDataMap, ARG_COLLECTION);
    String authToken = getJobDataString(jobDataMap, ARG_TOKEN);
    Supplier<String> userEmailSupplier = () -> samDao.getUserEmail(BearerToken.of(authToken));

    ImportDetails importDetails =
        new ImportDetails(
            jobId, userEmailSupplier, CollectionId.of(targetCollection), PrefixStrategy.PFB);

    // Import all the tables and rows inside the PFB.
    //
    // determine the workspace for the target collection
    WorkspaceId workspaceId = collectionService.getWorkspaceId(CollectionId.of(targetCollection));

    // Find all the snapshot ids in the PFB, then create or verify references from the
    // workspace to the snapshot for each of those snapshot ids.
    // This will throw an exception if there are policy conflicts between the workspace
    // and the snapshots.
    //
    // This is HTTP connection #1 to the PFB.
    logger.info("Finding snapshots in this PFB...");
    Set<UUID> snapshotIds = withPfbStream(url, this::findSnapshots);

    logger.info("Linking snapshots...");
    linkSnapshots(snapshotIds, workspaceId);

    // Import all the tables and rows inside the PFB.
    try (RecordSink recordSink = recordSinkFactory.buildRecordSink(importDetails)) {
      // This is HTTP connection #2 to the PFB.
      logger.info("Importing tables and rows from this PFB...");
      BatchWriteResult result =
          withPfbStream(url, stream -> importTables(stream, recordSink, BASE_ATTRIBUTES));

      // This is HTTP connection #3 to the PFB.
      logger.info("Updating tables and rows from this PFB with relations...");
      // TODO: merging batch results may have unexpected behavior until BatchWriteResult can
      //   group its merged results under import mode; most notably, relations will be double
      //   counted
      result.merge(withPfbStream(url, stream -> importTables(stream, recordSink, RELATIONS)));
    } catch (DataImportException e) {
      throw new PfbImportException(e.getMessage(), e);
    }

    // TODO(AJ-1453): save the result of importTables and persist them to the job
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
      throw new PfbParsingException("Error processing PFB: " + e.getMessage(), e);
    }
  }

  /**
   * Given a DataFileStream representing a PFB, import all the tables and rows inside that PFB.
   *
   * @param dataStream stream representing the PFB.
   * @param recordSink the {@link RecordSink} which directs the records to their destination
   * @param importMode indicating whether to import all data in the tables or only the relations
   */
  BatchWriteResult importTables(
      DataFileStream<GenericRecord> dataStream, RecordSink recordSink, ImportMode importMode) {
    BatchWriteResult result =
        batchWriteService.batchWrite(
            recordSourceFactory.forPfb(dataStream, importMode),
            recordSink,
            /* recordType= */ null, // record type is determined later
            /* primaryKey= */ ID_FIELD); // PFBs currently only use ID_FIELD as primary key

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
  Set<UUID> findSnapshots(DataFileStream<GenericRecord> dataStream) {
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
        .map(obj -> obj.get(SNAPSHOT_ID_IDENTIFIER)) // get the source_datarepo_snapshot_id value
        .filter(Objects::nonNull) // expect source_datarepo_snapshot_id to be non-null
        .map(obj -> maybeUuid(obj.toString()))
        .filter(Objects::nonNull) // find only the unique snapshotids
        .collect(Collectors.toSet());
  }

  /**
   * Given a list of snapshot ids, create references from the workspace to the snapshot for each id
   * that does not already have a reference.
   *
   * @param snapshotIds the list of snapshot ids to create or verify references.
   */
  protected void linkSnapshots(Set<UUID> snapshotIds, WorkspaceId workspaceId) {
    // list existing snapshots linked to this workspace
    SnapshotSupport snapshotSupport = snapshotSupportFactory.buildSnapshotSupport(workspaceId);
    snapshotSupport.linkSnapshots(snapshotIds);
  }

  @Nullable
  private UUID maybeUuid(String input) {
    try {
      return UUID.fromString(input);
    } catch (Exception e) {
      logger.warn("found unparseable snapshot id '{}' in PFB contents", input);
      return null;
    }
  }
}
