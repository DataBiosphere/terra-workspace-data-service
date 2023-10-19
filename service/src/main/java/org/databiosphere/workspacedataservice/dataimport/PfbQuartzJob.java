package org.databiosphere.workspacedataservice.dataimport;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import bio.terra.pfb.PfbReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Shell/starting point for PFB import via Quartz. */
@Component
public class PfbQuartzJob extends QuartzJob {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final JobDao jobDao;

  public PfbQuartzJob(JobDao jobDao) {
    this.jobDao = jobDao;
  }

  @Override
  protected JobDao getJobDao() {
    return this.jobDao;
  }

  @Override
  protected void executeInternal(UUID jobId, JobExecutionContext context) {
    // TODO: AJ-1227? implement PFB import.
    String url = (String) context.get(ARG_URL);
    Set<String> snapshotIds = Collections.emptySet();
    try (DataFileStream<GenericRecord> dataStream = PfbReader.getGenericRecordsStream(url)) {
      while (dataStream.hasNext()) {
        GenericRecord record = dataStream.next();
        record.get("object");
      }
    } catch (IOException e) {
      // Handle exceptions if necessary
      e.printStackTrace();
    }

    logger.info("TODO: implement PFB import.");
  }
}
