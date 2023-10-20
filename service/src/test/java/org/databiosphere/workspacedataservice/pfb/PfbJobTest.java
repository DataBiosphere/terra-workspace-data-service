package org.databiosphere.workspacedataservice.pfb;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.PfbQuartzJob;
import org.junit.jupiter.api.Test;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.impl.JobDetailImpl;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
public class PfbJobTest {

  @MockBean JobDao jobDao;

  @Test
  void snapshotIdsAreParsed() throws JobExecutionException {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);
    when(mockContext.getMergedJobDataMap())
        .thenReturn(
            new JobDataMap(
                Map.of(
                    ARG_TOKEN,
                    "expectedToken",
                    ARG_URL,
                    "/Users/bmorgan/dev/workbench/terra-workspace-data-service/service/src/test/resources/test.avro")));
    JobDetailImpl jobDetail = new JobDetailImpl();
    jobDetail.setKey(new JobKey(UUID.randomUUID().toString(), "bar"));
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    new PfbQuartzJob(jobDao).execute(mockContext);
  }
}
