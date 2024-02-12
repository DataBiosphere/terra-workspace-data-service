package org.databiosphere.workspacedataservice.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MvcResult;

@ActiveProfiles(profiles = {"mock-sam"})
@DirtiesContext
class JobControllerMockMvcTest extends MockMvcTestBase {
  @MockBean private JobDao jobDao;

  @Test
  void smokeTestGetJob() throws Exception {
    // return a test job from the mocked JobDao
    UUID jobId = UUID.randomUUID();
    UUID instanceId = UUID.randomUUID();
    GenericJobServerModel expected =
        new GenericJobServerModel(
            jobId,
            GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
            instanceId,
            GenericJobServerModel.StatusEnum.RUNNING,
            // set created and updated to now, but in UTC because that's how Postgres stores it
            OffsetDateTime.now(ZoneId.of("Z")),
            OffsetDateTime.now(ZoneId.of("Z")));
    when(jobDao.getJob(jobId)).thenReturn(expected);

    // calling the API should result in 202 Accepted
    MvcResult mvcResult =
        mockMvc
            .perform(get("/job/v1/{jobId}", expected.getJobId()))
            .andExpect(status().isAccepted())
            .andReturn();

    // and the API response should be a valid GenericJobServerModel
    GenericJobServerModel actual = fromJson(mvcResult, GenericJobServerModel.class);

    // which is equal to the expected job
    assertEquals(expected, actual);
  }
}
