package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_COLLECTION;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.SnapshotExportResponseModel;
import bio.terra.datarepo.model.SnapshotExportResponseModelFormatParquetLocationTables;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import io.micrometer.observation.ObservationRegistry;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.FileDownloadHelper;
import org.databiosphere.workspacedataservice.dataimport.WsmSnapshotSupport;
import org.databiosphere.workspacedataservice.jobexec.JobExecutionException;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.databiosphere.workspacedataservice.recordsink.RecordSinkFactory;
import org.databiosphere.workspacedataservice.recordsource.RecordSource.ImportMode;
import org.databiosphere.workspacedataservice.recordsource.RecordSourceFactory;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.service.model.exception.TdrManifestImportException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class TdrManifestQuartzJob extends QuartzJob {

  private final JobDao jobDao;
  private final WorkspaceManagerDao wsmDao;
  private final RestClientRetry restClientRetry;
  private final RecordSinkFactory recordSinkFactory;
  private final BatchWriteService batchWriteService;
  private final CollectionService collectionService;
  private final ActivityLogger activityLogger;
  private final ObjectMapper mapper;
  private final RecordSourceFactory recordSourceFactory;

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public TdrManifestQuartzJob(
      JobDao jobDao,
      WorkspaceManagerDao wsmDao,
      RestClientRetry restClientRetry,
      RecordSourceFactory recordSourceFactory,
      RecordSinkFactory recordSinkFactory,
      BatchWriteService batchWriteService,
      CollectionService collectionService,
      ActivityLogger activityLogger,
      ObjectMapper mapper,
      ObservationRegistry observationRegistry) {
    super(observationRegistry);
    this.jobDao = jobDao;
    this.wsmDao = wsmDao;
    this.restClientRetry = restClientRetry;
    this.recordSinkFactory = recordSinkFactory;
    this.recordSourceFactory = recordSourceFactory;
    this.batchWriteService = batchWriteService;
    this.collectionService = collectionService;
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
    UUID targetCollection = getJobDataUUID(jobDataMap, ARG_COLLECTION);

    // determine the workspace for the target collection
    WorkspaceId workspaceId = collectionService.getWorkspaceId(CollectionId.of(targetCollection));

    // read manifest
    SnapshotExportResponseModel snapshotExportResponseModel = parseManifest(url);

    // get the snapshot id from the manifest
    UUID snapshotId = snapshotExportResponseModel.getSnapshot().getId();
    logger.info("Starting import of snapshot {} to collection {}", snapshotId, targetCollection);

    // Create snapshot reference
    linkSnapshots(Set.of(snapshotId), workspaceId);

    // read the manifest and extract the information necessary to perform the import
    List<TdrManifestImportTable> tdrManifestImportTables =
        extractTableInfo(snapshotExportResponseModel, workspaceId);

    // get all the parquet files from the manifests

    FileDownloadHelper fileDownloadHelper = getFilesForImport(tdrManifestImportTables);

    // loop through the tables to be imported and upsert base attributes
    var result =
        importTables(
            tdrManifestImportTables,
            fileDownloadHelper.getFileMap(),
            targetCollection,
            ImportMode.BASE_ATTRIBUTES);

    // add relations to the existing base attributes
    importTables(
        tdrManifestImportTables,
        fileDownloadHelper.getFileMap(),
        targetCollection,
        ImportMode.RELATIONS);

    // activity logging for import status
    // no specific activity logging for relations since main import is a superset
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
    // delete temp files after everything else is completed
    // Any failed deletions will be removed if/when pod restarts
    fileDownloadHelper.deleteFileDirectory();
  }

  /**
   * Given a single Parquet file to be imported, import it
   *
   * @param inputFile Parquet file to be imported.
   * @param table info about the table to be imported
   * @param collectionId collection into which to import
   * @param importMode mode for this invocation
   * @return statistics on what was imported
   */
  @VisibleForTesting
  BatchWriteResult importTable(
      InputFile inputFile, TdrManifestImportTable table, UUID collectionId, ImportMode importMode) {
    // upsert this parquet file's contents
    try (ParquetReader<GenericRecord> avroParquetReader =
        AvroParquetReader.<GenericRecord>builder(inputFile)
            .set(READ_INT96_AS_FIXED, "true")
            .build()) {
      logger.info("batch-writing records for file ...");
      return batchWriteService.batchWrite(
          recordSourceFactory.forTdrImport(avroParquetReader, table, importMode),
          recordSinkFactory.buildRecordSink(collectionId, /* prefix= */ "tdr"),
          table.recordType(),
          table.primaryKey());
    } catch (Throwable t) {
      throw new TdrManifestImportException(t.getMessage(), t);
    }
  }

  /**
   * Given the list of tables/data files to be imported, loop through and import each one
   *
   * @param importTables tables to be imported
   * @param targetCollection collection into which to import
   * @param importMode mode for this invocation
   */
  private BatchWriteResult importTables(
      List<TdrManifestImportTable> importTables,
      Multimap<String, File> fileMap,
      UUID targetCollection,
      ImportMode importMode) {

    var combinedResult = BatchWriteResult.empty();
    // loop through the tables that have data files.
    importTables.forEach(
        importTable -> {
          logger.info("Processing table '{}' ...", importTable.recordType().getName());

          // loop through each parquet file
          fileMap
              .get(importTable.recordType().getName())
              .forEach(
                  file -> {
                    try {
                      org.apache.hadoop.fs.Path hadoopFilePath =
                          new org.apache.hadoop.fs.Path(file.toString());
                      Configuration configuration = new Configuration();

                      // generate the HadoopInputFile
                      InputFile inputFile = HadoopInputFile.fromPath(hadoopFilePath, configuration);
                      var result =
                          importTable(inputFile, importTable, targetCollection, importMode);
                      if (result != null) {
                        combinedResult.merge(result);
                      }
                    } catch (IOException e) {
                      throw new TdrManifestImportException(e.getMessage(), e);
                    }
                  });
        });
    return combinedResult;
  }

  /**
   * Given the list of tables/data files to be imported, loop through and download each one to a
   * temporary file
   *
   * @param importTables tables to be imported
   * @return path for the directory where downloaded files are located
   */
  @VisibleForTesting
  FileDownloadHelper getFilesForImport(List<TdrManifestImportTable> importTables) {
    try {
      FileDownloadHelper fileDownloadHelper = new FileDownloadHelper("tempParquetDir");

      // loop through the tables that have data files.
      importTables.forEach(
          importTable -> {
            logger.info("Fetching files for table '{}' ...", importTable.recordType().getName());

            // find all Parquet files for this table
            List<URL> paths = importTable.dataFiles();
            logger.debug(
                "Table '{}' has {} export file(s) ...",
                importTable.recordType().getName(),
                paths.size());

            // loop through each parquet file
            paths.forEach(
                path ->
                    fileDownloadHelper.downloadFileFromURL(
                        importTable.recordType().getName(), path));
          });

      return fileDownloadHelper;
    } catch (IOException e) {
      throw new TdrManifestImportException("Error downloading temporary files", e);
    }
  }

  /**
   * Read the manifest from the user-specified URL into a SnapshotExportResponseModel java object
   *
   * @param manifestUrl url to the manifest
   * @return parsed object
   */
  @VisibleForTesting
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
  @VisibleForTesting
  List<TdrManifestImportTable> extractTableInfo(
      SnapshotExportResponseModel snapshotExportResponseModel, WorkspaceId workspaceId) {

    WsmSnapshotSupport wsmSnapshotSupport =
        new WsmSnapshotSupport(workspaceId, wsmDao, restClientRetry, activityLogger);

    // find all the exported tables in the manifest.
    // This is the format.parquet.location.tables section in the manifest
    List<SnapshotExportResponseModelFormatParquetLocationTables> tables =
        snapshotExportResponseModel.getFormat().getParquet().getLocation().getTables();

    // find the primary keys for each table.
    // This is the snapshot.tables section in the manifest
    Map<RecordType, String> primaryKeys =
        wsmSnapshotSupport.identifyPrimaryKeys(
            snapshotExportResponseModel.getSnapshot().getTables());

    // TODO(AJ-1536): get the types for each table from the manifest; these will be needed after
    //   Avro deserialization to convert them to the correct types.

    // find the relations for each table.
    // This is the snapshot.relationships section in the manifest
    Multimap<RecordType, RelationshipModel> relationsByTable =
        wsmSnapshotSupport.identifyRelations(
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

  @VisibleForTesting
  protected URL parseUrl(String path) {
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
  protected void linkSnapshots(Set<UUID> snapshotIds, WorkspaceId workspaceId) {
    // list existing snapshots linked to this workspace
    WsmSnapshotSupport wsmSnapshotSupport =
        new WsmSnapshotSupport(workspaceId, wsmDao, restClientRetry, activityLogger);
    // TODO AJ-1673: don't use the env-var workspaceId here
    wsmSnapshotSupport.linkSnapshots(snapshotIds);
  }
}
