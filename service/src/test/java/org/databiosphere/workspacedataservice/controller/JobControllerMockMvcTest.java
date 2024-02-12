package org.databiosphere.workspacedataservice.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.shared.model.InstanceId;
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

  @Test
  void smokeTestJobsInInstanceV1() throws Exception {
    // return a test job from the mocked JobDao
    UUID jobId1 = UUID.randomUUID();
    UUID jobId2 = UUID.randomUUID();
    InstanceId instanceId = new InstanceId(UUID.randomUUID());
    // set created and updated to now, but in UTC because that's how Postgres stores it
    OffsetDateTime time = OffsetDateTime.now(ZoneId.of("Z"));

    List<GenericJobServerModel> expected = new ArrayList<GenericJobServerModel>(2);
    expected.add(
        new GenericJobServerModel(
                jobId1,
                GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
                GenericJobServerModel.StatusEnum.RUNNING,
                time,
                time)
            .instanceId(instanceId.id()));
    expected.add(
        new GenericJobServerModel(
                jobId2,
                GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
                GenericJobServerModel.StatusEnum.RUNNING,
                time,
                time)
            .instanceId(instanceId.id()));
    when(jobDao.getJobsForInstance(instanceId, Arrays.asList("RUNNING"))).thenReturn(expected);

    // calling the API should result in 200 OK
    MvcResult mvcResult =
        mockMvc
            .perform(
                get("/job/v1/instance/{instanceUuid}?statuses={statuses}", instanceId, "RUNNING"))
            .andExpect(status().isOk())
            .andReturn();

    // and the API response should be a valid GenericJobServerModel list (array)
    GenericJobServerModel[] actual = fromJson(mvcResult, GenericJobServerModel[].class);

    // which is equal to the expected job list
    assertTrue(expected.containsAll(Arrays.asList(actual)));
    assertEquals(expected.size(), actual.length);

    // all jobs in both lists should have the same instanceId
    assertEquals(expected.get(0).getInstanceId(), actual[1].getInstanceId());
  }
}
