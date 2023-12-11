package org.databiosphere.workspacedataservice.dataimport;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_INSTANCE;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.SnapshotExportResponseModel;
import bio.terra.datarepo.model.SnapshotExportResponseModelFormatParquetLocationTables;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.avro.generic.GenericRecord;
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
          paths.forEach(
              path -> {
                logger.info("opening reader for file ...");
                InputFile inputFile;
                // open the parquet file for reading
                try {
                  // TODO AJ-1013: auth? If this is only signed urls, there's no problem.
                  inputFile = HadoopInputFile.fromPath(new Path(path), new Configuration());
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
                // upsert this parquet file's contents
                try (ParquetReader<GenericRecord> avroParquetReader =
                    AvroParquetReader.genericRecordReader(inputFile)) {

                  logger.info("batch-writing records for file ...");

                  // TODO AJ-1013 pass in which columns are relations
                  batchWriteService.batchWriteParquetStream(
                      avroParquetReader,
                      instanceId,
                      recordType,
                      Optional.of(pk),
                      PfbStreamWriteHandler.PfbImportMode.BASE_ATTRIBUTES);

                  // TODO AJ-1013: activity logging

                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
        });

    // TODO AJ-1013 upsert relation columns

    logger.info(tables.toString());
    // TODO AJ-1013 re-evaluate dataRepoService.importSnapshot, should it be removed?
  }
}
