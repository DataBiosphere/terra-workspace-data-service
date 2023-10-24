package org.databiosphere.workspacedataservice.dataimport;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.pfb.PfbReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Shell/starting point for PFB import via Quartz. */
@Component
public class PfbQuartzJob extends QuartzJob {

  public static final String SNAPSHOT_ID_IDENTIFIER = "source_datarepo_snapshot_id";

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final JobDao jobDao;
  private final WorkspaceManagerDao wsmDao;

  public PfbQuartzJob(JobDao jobDao, WorkspaceManagerDao wsmDao) {
    this.jobDao = jobDao;
    this.wsmDao = wsmDao;
  }

  @Override
  protected JobDao getJobDao() {
    return this.jobDao;
  }

  @Override
  protected void executeInternal(UUID jobId, JobExecutionContext context) {
    String url = (String) context.get(ARG_URL);
    Set<String> snapshotIds = new HashSet();
    try (DataFileStream<GenericRecord> dataStream = PfbReader.getGenericRecordsStream(url)) {
      while (dataStream.hasNext()) {
        GenericRecord record = dataStream.next();
        // Records in a pfb are stored under the key "object"
        GenericRecord recordObject = (GenericRecord) record.get("object");
        if (recordObject == null) {
          // TODO what type of exception/what message
          throw new RuntimeException("Record in pfb missing data");
        }
        if (recordObject.hasField(SNAPSHOT_ID_IDENTIFIER)) {
          Object snapId = recordObject.get(SNAPSHOT_ID_IDENTIFIER);
          if (snapId != null) {
            snapshotIds.add(snapId.toString());
          }
        }
      }
    } catch (IOException e) {
      // Handle exceptions if necessary
      e.printStackTrace();
    }

    // TODO AJ-1371 pass snapshotIds to WSM
    for (String id : snapshotIds) {
      wsmDao.createDataRepoSnapshotReference(new SnapshotModel().id(UUID.fromString(id)));
    }
    // TODO: AJ-1227 implement PFB import.
    logger.info("TODO: implement PFB import.");
  }
}
