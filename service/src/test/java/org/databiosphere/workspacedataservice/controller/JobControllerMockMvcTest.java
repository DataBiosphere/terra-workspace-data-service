package org.databiosphere.workspacedataservice.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@ActiveProfiles(profiles = {"mock-instance-dao", "mock-sam"})
@DirtiesContext
@SpringBootTest
@AutoConfigureMockMvc
public class JobControllerMockMvcTest {
  @Autowired private MockMvc mockMvc;
  @Autowired private ObjectMapper mapper;
  @Autowired private InstanceDao instanceDao;
  @MockBean private JobDao jobDao;

  @Test
  public void smokeTestCreateImport() throws Exception {
    // create the instance in the MockInstanceDao
    UUID instanceId = UUID.randomUUID();
    instanceDao.createSchema(instanceId);

    // return a test job from the mocked JobDao
    UUID jobId = UUID.randomUUID();
    GenericJobServerModel expected =
        new GenericJobServerModel(
            jobId,
            GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
            GenericJobServerModel.StatusEnum.RUNNING,
            OffsetDateTime.now(),
            OffsetDateTime.now());
    when(jobDao.getJob(eq(jobId))).thenReturn(expected);

    // calling the API should result in 202 Accepted
    MvcResult mvcResult =
        mockMvc
            .perform(get("/{instanceUuid}/job/v1/{jobId}", instanceId, expected.getJobId()))
            .andExpect(status().isAccepted())
            .andReturn();

    // and the API response should be a valid GenericJobServerModel
    GenericJobServerModel actual =
        mapper.readValue(mvcResult.getResponse().getContentAsString(), GenericJobServerModel.class);

    // which is equal to the expected job
    // TODO: this is failing due to comparisons on created/updated times, due to timezone issue
    assertEquals(expected, actual);
  }
}
