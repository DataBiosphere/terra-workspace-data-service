package org.databiosphere.workspacedataservice.dataimport;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_INSTANCE;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.SnapshotExportResponseModel;
import bio.terra.datarepo.model.SnapshotExportResponseModelFormatParquetLocationTables;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
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
import org.databiosphere.workspacedataservice.service.DataRepoService;
import org.databiosphere.workspacedataservice.service.PfbStreamWriteHandler;
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
  private final DataRepoService dataRepoService; // TODO AJ-1013 do we need this dao?

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public TdrManifestQuartzJob(
      JobDao jobDao,
      WorkspaceManagerDao wsmDao,
      RestClientRetry restClientRetry,
      BatchWriteService batchWriteService,
      ActivityLogger activityLogger,
      @Value("${twds.instance.workspace-id}") UUID workspaceId,
      ObjectMapper mapper,
      DataRepoService dataRepoService) {
    this.jobDao = jobDao;
    this.wsmDao = wsmDao;
    this.restClientRetry = restClientRetry;
    this.workspaceId = workspaceId;
    this.batchWriteService = batchWriteService;
    this.activityLogger = activityLogger;
    this.mapper = mapper;
    this.dataRepoService = dataRepoService;
  }

  @Override
  protected JobDao getJobDao() {
    return this.jobDao;
  }

  // TODO AJ-1013 unit tests
  @Override
  protected void executeInternal(UUID jobId, JobExecutionContext context) {
    // retrieve the Quartz JobDataMap, which contains arguments for this execution
    JobDataMap jobDataMap = context.getMergedJobDataMap();

    // get instanceid from job data
    UUID instanceId = getJobDataUUID(jobDataMap, ARG_INSTANCE);
    // get auth token
    String token = getJobDataString(jobDataMap, ARG_TOKEN);
    // get the TDR manifest url from job data
    URL url = getJobDataUrl(jobDataMap, ARG_URL);

    /*
      + 1. read manifest, find snapshot id
      2. create reference from workspace to snapshot
      3. find all tables and primary keys for each table. Potentially read the whole schema here.
      4. Identify all the parquet files for each table.
      5. Read each parquet file into Avro java objects
      6. Insert base attributes for each parquet file, using recordType=tableName and id=primary key from schema
      7. Second pass to insert relations

    */

    // read manifest
    SnapshotExportResponseModel snapshotExportResponseModel;
    try {
      snapshotExportResponseModel = mapper.readValue(url, SnapshotExportResponseModel.class);
    } catch (IOException e) {
      throw new JobExecutionException(
          "Error reading TDR snapshot manifest: %s".formatted(e.getMessage()), e);
    }

    // get the snapshot id from the manifest
    UUID snapshotId = snapshotExportResponseModel.getSnapshot().getId();
    logger.info("Starting import of snapshot {} to instance {}", snapshotId, instanceId);

    TdrSnapshotSupport tdrSnapshotSupport =
        new TdrSnapshotSupport(workspaceId, wsmDao, restClientRetry);

    // TODO AJ-1013 create snapshot reference - use linkSnapshots from PfbQuartzJob

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
    Multimap<RecordType, RelationshipModel> relations =
        tdrSnapshotSupport.identifyRelations(
            snapshotExportResponseModel.getSnapshot().getRelationships());

    // loop through the exported tables and process the parquet files for each one
    tables.forEach(
        table -> {
          // record type and primary key for this table
          RecordType recordType = RecordType.valueOf(table.getName());
          String pk =
              primaryKeys.getOrDefault(recordType, tdrSnapshotSupport.getDefaultPrimaryKey());

          logger.info("Processing table '{}' ...", table.getName());
          List<String> paths = table.getPaths();

          logger.info("Table '{}' has {} export file(s) ...", table.getName(), paths.size());
          paths.forEach(path -> logger.info("    --> " + path));

          paths.forEach(
              encodedPath -> {
                // TODO AJ-1013: temp hack to work around TDR bug
                String incorrectlyEncodedPart = encodedPath.substring(0, encodedPath.indexOf('?'));
                String correctlyEncodedPart =
                    java.net.URLDecoder.decode(incorrectlyEncodedPart, Charset.defaultCharset());
                String path = encodedPath.replace(incorrectlyEncodedPart, correctlyEncodedPart);

                //                URI uri = URI.create(encodedPath);
                //                String encoded = uri.getPath();
                //                String decoded = java.net.URLDecoder.decode(encoded,
                // Charset.defaultCharset());
                //                String path = encodedPath.replace(encoded, decoded);
                // TODO AJ-1013: END temp hack to work around TDR bug
                try {

                  // TODO AJ-1013 access the URL directly, instead of downloading to a temp file?
                  File tempFile = File.createTempFile("tdr-", "download");
                  FileUtils.copyURLToFile(new URL(path), tempFile);
                  Path hadoopFilePath = new Path(tempFile.getPath());
                  logger.info("opening reader for file {} ...", path);
                  logger.info(
                      "hadoopFilePath for this file is {} ...", hadoopFilePath.toUri().toString());

                  InputFile inputFile;
                  // open the parquet file for reading
                  //                String containerName = "c1d4b239-3a19-473b-a075-a9c9fbcc9e5c";
                  //                String accountName = "tdrdevwdcmeqdsmccbzepcwd";
                  try {
                    Configuration configuration = new Configuration();
                    //                  configuration.set("fs.azure",
                    // "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
                    //                  configuration.set("fs.azure.authorization", token);
                    //                  configuration.set(
                    //
                    // "fs.azure.sas.containerName.tdrdevwdcmeqdsmccbzepcwd.blob.core.windows.net",
                    //                      token);

                    // hdfs.DistributedFileSystem

                    // TODO AJ-1013: auth? If this is only signed urls, there's no problem.
                    inputFile = HadoopInputFile.fromPath(hadoopFilePath, configuration);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }

                  // upsert this parquet file's contents
                  try (ParquetReader<GenericRecord> avroParquetReader =
                      AvroParquetReader.<GenericRecord>builder(inputFile).build()) {

                    logger.info("batch-writing records for file ...");

                    // TODO AJ-1013 pass in which columns are relations
                    batchWriteService.batchWriteParquetStream(
                        avroParquetReader,
                        instanceId,
                        recordType,
                        pk,
                        PfbStreamWriteHandler.PfbImportMode.BASE_ATTRIBUTES);

                    // TODO AJ-1013: activity logging

                  } catch (IOException e) {
                    logger.error(
                        "Hit an error on file {}: {}",
                        hadoopFilePath.toUri().toString(),
                        e.getMessage(),
                        e);
                  }
                } catch (Throwable t) {
                  logger.error("Hit an error on file {}: {}. Continuing.", path, t.getMessage());
                }
              });
        });

    // TODO AJ-1013 upsert relation columns

    logger.info(tables.toString());
    // TODO AJ-1013 re-evaluate dataRepoService.importSnapshot, should it be removed?
  }
}
