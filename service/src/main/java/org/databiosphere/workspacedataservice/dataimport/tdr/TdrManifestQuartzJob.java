package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_INSTANCE;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.SnapshotExportResponseModel;
import bio.terra.datarepo.model.SnapshotExportResponseModelFormatParquetLocationTables;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.WsmSnapshotSupport;
import org.databiosphere.workspacedataservice.jobexec.JobExecutionException;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.databiosphere.workspacedataservice.recordstream.TwoPassStreamingWriteHandler;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
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

    // get all the parquet files from the manifests
    // TODO do we really want to download them all at once ahead of time?
    java.nio.file.Path fileDir = getFilesForImport(tdrManifestImportTables);

    // loop through the tables to be imported and upsert base attributes
    var result =
        importTables(
            tdrManifestImportTables,
            fileDir,
            targetInstance,
            TwoPassStreamingWriteHandler.ImportMode.BASE_ATTRIBUTES);

    // add relations to the existing base attributes
    importTables(
        tdrManifestImportTables,
        fileDir,
        targetInstance,
        TwoPassStreamingWriteHandler.ImportMode.RELATIONS);

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
    try {
      FileUtils.deleteDirectory(fileDir.toFile());
    } catch (IOException e) {
      logger.error("Unable to delete temporary files", e);
    }
  }

  /**
   * Given a single Parquet file to be imported, import it
   *
   * @param inputFile Parquet file to be imported.
   * @param table info about the table to be imported
   * @param targetInstance instance into which to import
   * @param importMode mode for this invocation
   * @return statistics on what was imported
   */
  @VisibleForTesting
  BatchWriteResult importTable(
      InputFile inputFile,
      TdrManifestImportTable table,
      UUID targetInstance,
      TwoPassStreamingWriteHandler.ImportMode importMode) {
    // upsert this parquet file's contents
    try (ParquetReader<GenericRecord> avroParquetReader =
        AvroParquetReader.<GenericRecord>builder(inputFile)
            .set(READ_INT96_AS_FIXED, "true")
            .build()) {
      logger.info("batch-writing records for file ...");

      BatchWriteResult result =
          batchWriteService.batchWriteParquetStream(
              avroParquetReader, targetInstance, table, importMode);

      return result;
    } catch (Throwable t) {
      logger.error("Hit an error on file: {}", t.getMessage());
      throw new TdrManifestImportException(t.getMessage());
    }
  }

  /**
   * Given the list of tables/data files to be imported, loop through and import each one
   *
   * @param importTables tables to be imported
   * @param targetInstance instance into which to import
   * @param importMode mode for this invocation
   */
  private BatchWriteResult importTables(
      List<TdrManifestImportTable> importTables,
      Path fileDir,
      UUID targetInstance,
      TwoPassStreamingWriteHandler.ImportMode importMode) {

    var combinedResult = BatchWriteResult.empty();
    // loop through the tables that have data files.
    importTables.forEach(
        importTable -> {
          try {
            logger.info("Processing table '{}' ...", importTable.recordType().getName());

            Path subDirectory = fileDir.resolve(importTable.recordType().getName());

            // loop through each parquet file
            Files.walk(subDirectory)
                .forEach(
                    file -> {
                      try {
                        org.apache.hadoop.fs.Path hadoopFilePath =
                            new org.apache.hadoop.fs.Path(file.toString());
                        Configuration configuration = new Configuration();

                        // generate the HadoopInputFile
                        InputFile inputFile =
                            HadoopInputFile.fromPath(hadoopFilePath, configuration);
                        var result =
                            importTable(inputFile, importTable, targetInstance, importMode);
                        if (result != null) {
                          combinedResult.merge(result);
                        }
                      } catch (IOException e) {
                        throw new TdrManifestImportException(e.getMessage(), e);
                      }
                    });
          } catch (IOException e) {
            throw new TdrManifestImportException(e.getMessage(), e);
          }
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
  java.nio.file.Path getFilesForImport(List<TdrManifestImportTable> importTables) {
    try {
      // create a temporary subdirectory to store download files
      java.nio.file.Path tempDir = Files.createTempDirectory("tempParquetDir");
      // Permissions to add to files to make them read-only
      Set<PosixFilePermission> permissions =
          EnumSet.of(
              PosixFilePermission.OWNER_READ,
              PosixFilePermission.GROUP_READ,
              PosixFilePermission.OTHERS_READ);

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

            try {
              // Create a subdirectory for the files for this table
              java.nio.file.Path subDir =
                  Files.createDirectory(tempDir.resolve(importTable.recordType().getName()));

              // loop through each parquet file
              paths.forEach(
                  path -> {
                    try {
                      // download the file from the URL to a temp file on the local filesystem
                      // Azure urls, with SAS tokens, don't need any particular auth.
                      File tempFile =
                          File.createTempFile(
                              /* prefix= */ "tdr-", /* suffix= */ "download", subDir.toFile());
                      logger.info("downloading to temp file {} ...", tempFile.getPath());
                      FileUtils.copyURLToFile(path, tempFile);
                      // In the TDR manifest, for Azure snapshots only,
                      // the first file in the list will always be a directory. Attempting to import
                      // that directory
                      // will fail; it has no content. To avoid those failures,
                      // check files for length and ignore any that are empty
                      if (tempFile.length() == 0) {
                        logger.info("Empty file in parquet, skipping");
                        tempFile.delete();
                      } else {
                        // Once the remote file has been copied to the temp file, make it read-only
                        Files.setPosixFilePermissions(tempFile.toPath(), permissions);
                      }

                    } catch (IOException e) {
                      throw new TdrManifestImportException(e.getMessage(), e);
                    }
                  });
            } catch (IOException e) {
              throw new TdrManifestImportException(e.getMessage(), e);
            }
          });

      return tempDir;
    } catch (IOException e) {
      throw new TdrManifestImportException(e.getMessage(), e);
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
      SnapshotExportResponseModel snapshotExportResponseModel) {

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
  protected void linkSnapshots(Set<UUID> snapshotIds) {
    // list existing snapshots linked to this workspace
    WsmSnapshotSupport wsmSnapshotSupport =
        new WsmSnapshotSupport(workspaceId, wsmDao, restClientRetry, activityLogger);
    wsmSnapshotSupport.linkSnapshots(snapshotIds);
  }
}
