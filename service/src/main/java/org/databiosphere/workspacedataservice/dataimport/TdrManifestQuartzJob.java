package org.databiosphere.workspacedataservice.dataimport;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_INSTANCE;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.SnapshotExportResponseModel;
import bio.terra.datarepo.model.SnapshotExportResponseModelFormatParquetLocationTables;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.jobexec.JobExecutionException;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.TwoPassStreamingWriteHandler;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.service.model.exception.TdrManifestImportException;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TdrManifestQuartzJob extends QuartzJob {

  private final JobDao jobDao;
  private final WorkspaceManagerDao wsmDao;
  private final RestClientRetry restClientRetry;
  private final UUID workspaceId;
  private final BatchWriteService batchWriteService;
  private final ActivityLogger activityLogger;
  private final ObjectMapper mapper;

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public TdrManifestQuartzJob(
      JobDao jobDao,
      WorkspaceManagerDao wsmDao,
      RestClientRetry restClientRetry,
      BatchWriteService batchWriteService,
      ActivityLogger activityLogger,
      @Value("${twds.instance.workspace-id}") UUID workspaceId,
      ObjectMapper mapper) {
    this.jobDao = jobDao;
    this.wsmDao = wsmDao;
    this.restClientRetry = restClientRetry;
    this.workspaceId = workspaceId;
    this.batchWriteService = batchWriteService;
    this.activityLogger = activityLogger;
    this.mapper = mapper;
  }

  @Override
  protected JobDao getJobDao() {
    return this.jobDao;
  }

  // TODO AJ-1523 unit tests
  @Override
  protected void executeInternal(UUID jobId, JobExecutionContext context) {
    // Grab the manifest url from the job's data map
    JobDataMap jobDataMap = context.getMergedJobDataMap();
    URL url = getJobDataUrl(jobDataMap, ARG_URL);
    UUID targetInstance = getJobDataUUID(jobDataMap, ARG_INSTANCE);

    // read manifest
    SnapshotExportResponseModel snapshotExportResponseModel = parseManifest(url);

    // get the snapshot id from the manifest
    UUID snapshotId = snapshotExportResponseModel.getSnapshot().getId();
    logger.info("Starting import of snapshot {} to instance {}", snapshotId, targetInstance);

    // Create snapshot reference
    linkSnapshots(Set.of(snapshotId));

    // read the manifest and extract the information necessary to perform the import
    List<TdrManifestImportTable> tdrManifestImportTables =
        extractTableInfo(snapshotExportResponseModel);

    // activity logged - note start of import
    activityLogger.saveEventForCurrentUser(
        user -> user.started().importTables().withUuid(snapshotId));

    // loop through the tables to be imported and upsert base attributes
    importTables(
        tdrManifestImportTables,
        targetInstance,
        TwoPassStreamingWriteHandler.ImportMode.BASE_ATTRIBUTES);

    // add relations to the existing base attributes
    importTables(
        tdrManifestImportTables, targetInstance, TwoPassStreamingWriteHandler.ImportMode.RELATIONS);

    // activity logger - note end of import, if this event is not seen
    // it would be safe to assume that import didn't complete successfully
    activityLogger.saveEventForCurrentUser(
        user -> user.completed().importTables().withUuid(snapshotId));
  }

  /**
   * Given a single Parquet file to be imported, import it
   *
   * @param path path to Parquet file to be imported.
   * @param table info about the table to be imported
   * @param targetInstance instance into which to import
   * @param importMode mode for this invocation
   * @return statistics on what was imported
   */
  private BatchWriteResult importTable(
      URL path,
      TdrManifestImportTable table,
      UUID targetInstance,
      TwoPassStreamingWriteHandler.ImportMode importMode) {
    // In the TDR manifest, for Azure snapshots only,
    // the first file in the list will always be a directory. Attempting to import that directory
    // will fail; it has no content. To ensure we are resilient to those failures, wrap everything
    // in try/catch.
    // TODO AJ-1518 more specific handling for 0-length directories; other errors should
    //     be true failures
    try {
      // download the file from the URL to a temp file on the local filesystem
      // Azure urls, with SAS tokens, don't need any particular auth.
      // TODO AJ-1517 can we access the URL directly, no temp file?
      File tempFile = File.createTempFile("tdr-", "download");
      logger.info("downloading to temp file {} ...", tempFile.getPath());
      FileUtils.copyURLToFile(path, tempFile);
      Path hadoopFilePath = new Path(tempFile.getPath());

      // generate the HadoopInputFile
      InputFile inputFile;
      try {
        // do we need any other config here?
        Configuration configuration = new Configuration();
        inputFile = HadoopInputFile.fromPath(hadoopFilePath, configuration);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // upsert this parquet file's contents
      try (ParquetReader<GenericRecord> avroParquetReader =
          AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
        logger.info("batch-writing records for file ...");

        BatchWriteResult result =
            batchWriteService.batchWriteParquetStream(
                avroParquetReader, targetInstance, table, importMode);

        // activity logging - mark what records were imported
        // this event doesn't indicate that the whole import failed, but it allows to have a
        // breadcrumb
        // to hopefully see which tables made the import fail if a failure does occur
        // there is a separate event that tracks overall start and finish of the whole import inside
        // executeInternal
        if (result != null) {
          result
              .entrySet()
              .forEach(
                  entry -> {
                    activityLogger.saveEventForCurrentUser(
                        user ->
                            user.upserted()
                                .record()
                                .withRecordType(entry.getKey())
                                .ofQuantity(entry.getValue()));
                  });
        }
        return result;
      } catch (IOException e) {
        logger.error("Hit an error on file: {}", e.getMessage(), e);
        return new BatchWriteResult(Map.of());
        // throw new TdrManifestImportException(e.getMessage());
        // TODO AJ-1518 more specific handling for 0-length directories; other errors should
        //     be true failures
      }
    } catch (Throwable t) {
      logger.error("Hit an error on file: {}. Continuing.", t.getMessage());
      return new BatchWriteResult(Map.of());
      // throw new TdrManifestImportException(t.getMessage());
      // TODO AJ-1518 more specific handling for 0-length directories; other errors should
      //     be true failures
    }
  }

  /**
   * Given the list of tables/data files to be imported, loop through and import each one
   *
   * @param importTables tables to be imported
   * @param targetInstance instance into which to import
   * @param importMode mode for this invocation
   */
  private void importTables(
      List<TdrManifestImportTable> importTables,
      UUID targetInstance,
      TwoPassStreamingWriteHandler.ImportMode importMode) {
    // loop through the tables that have data files.
    importTables.forEach(
        importTable -> {
          logger.info("Processing table '{}' ...", importTable.recordType().getName());

          // find all Parquet files for this table
          List<URL> paths = importTable.dataFiles();
          logger.debug(
              "Table '{}' has {} export file(s) ...",
              importTable.recordType().getName(),
              paths.size());

          // loop through each parquet file
          paths.forEach(
              path -> {
                importTable(path, importTable, targetInstance, importMode);
              });
        });
  }

  /**
   * Read the manifest from the user-specified URL into a SnapshotExportResponseModel java object
   *
   * @param manifestUrl url to the manifest
   * @return parsed object
   */
  SnapshotExportResponseModel parseManifest(URL manifestUrl) {
    // read manifest
    try {
      return mapper.readValue(manifestUrl, SnapshotExportResponseModel.class);
    } catch (IOException e) {
      throw new JobExecutionException(
          "Error reading TDR snapshot manifest: %s".formatted(e.getMessage()), e);
    }
  }

  /**
   * Given a SnapshotExportResponseModel, extract relevant info about the tables being imported.
   *
   * @param snapshotExportResponseModel the inbound manifest
   * @return information necessary for the import
   */
  List<TdrManifestImportTable> extractTableInfo(
      SnapshotExportResponseModel snapshotExportResponseModel) {

    TdrSnapshotSupport tdrSnapshotSupport =
        new TdrSnapshotSupport(workspaceId, wsmDao, restClientRetry, activityLogger);

    // find all the exported tables in the manifest.
    // This is the format.parquet.location.tables section in the manifest
    List<SnapshotExportResponseModelFormatParquetLocationTables> tables =
        snapshotExportResponseModel.getFormat().getParquet().getLocation().getTables();

    // find the primary keys for each table.
    // This is the snapshot.tables section in the manifest
    Map<RecordType, String> primaryKeys =
        tdrSnapshotSupport.identifyPrimaryKeys(
            snapshotExportResponseModel.getSnapshot().getTables());

    // find the relations for each table.
    // This is the snapshot.relationships section in the manifest
    Multimap<RecordType, RelationshipModel> relationsByTable =
        tdrSnapshotSupport.identifyRelations(
            snapshotExportResponseModel.getSnapshot().getRelationships());

    return tables.stream()
        .map(
            table -> {
              RecordType recordType = RecordType.valueOf(table.getName());
              // primary key must have already been calculated; if not, it's an inconsistency
              // in the manifest file itself
              if (!primaryKeys.containsKey(recordType)) {
                throw new TdrManifestImportException(
                    "Table %s with data files is unknown to the snapshot model"
                        .formatted(recordType.getName()));
              }
              String primaryKey = primaryKeys.get(recordType);

              // this table may or may not have relations
              Collection<RelationshipModel> possibleRelations = relationsByTable.get(recordType);

              // filter relations to those that point at a valid primary key
              List<RelationshipModel> relations =
                  possibleRelations.stream()
                      .filter(relationshipModel -> isValidRelation(relationshipModel, primaryKeys))
                      .toList();

              // determine data files for this table
              List<URL> dataFiles = table.getPaths().stream().map(this::parseUrl).toList();
              return new TdrManifestImportTable(recordType, primaryKey, dataFiles, relations);
            })
        .toList();
  }

  private boolean isValidRelation(
      RelationshipModel relationshipModel, Map<RecordType, String> primaryKeys) {
    // the target table and column requested by the snapshot model
    String requestedTable = relationshipModel.getTo().getTable();
    String requestedPrimaryKey = relationshipModel.getTo().getColumn();
    // the actual primary key for the target table
    String actualPrimaryKey = primaryKeys.get(RecordType.valueOf(requestedTable));
    return requestedPrimaryKey != null && requestedPrimaryKey.equals(actualPrimaryKey);
  }

  private URL parseUrl(String path) {
    try {
      return new URL(path);
    } catch (MalformedURLException e) {
      throw new TdrManifestImportException(e.getMessage(), e);
    }
  }

  /**
   * Given a list of snapshot ids, create references from the workspace to the snapshot for each id
   * that does not already have a reference.
   *
   * @param snapshotIds the list of snapshot ids to create or verify references.
   */
  protected void linkSnapshots(Set<UUID> snapshotIds) {
    // list existing snapshots linked to this workspace
    TdrSnapshotSupport tdrSnapshotSupport =
        new TdrSnapshotSupport(workspaceId, wsmDao, restClientRetry, activityLogger);
    tdrSnapshotSupport.linkSnapshots(snapshotIds);
  }
}
