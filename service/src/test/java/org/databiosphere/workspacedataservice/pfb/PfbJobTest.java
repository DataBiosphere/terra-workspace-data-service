package org.databiosphere.workspacedataservice.pfb;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.client.ApiException;
import bio.terra.workspace.model.DataRepoSnapshotAttributes;
import bio.terra.workspace.model.ResourceAttributesUnion;
import bio.terra.workspace.model.ResourceDescription;
import bio.terra.workspace.model.ResourceList;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.PfbQuartzJob;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.impl.JobDetailImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
class PfbJobTest {

  @MockBean JobDao jobDao;
  @MockBean WorkspaceManagerDao wsmDao;
  @Autowired RestClientRetry restClientRetry;

  @Test
  void doNotFailOnMissingSnapshotId() throws JobExecutionException {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);
    // This uses a non-TDR file so does not have the snapshotId
    URL resourceUrl = getClass().getResource("/minimal_data.avro");
    when(mockContext.getMergedJobDataMap())
        .thenReturn(
            new JobDataMap(Map.of(ARG_TOKEN, "expectedToken", ARG_URL, resourceUrl.toString())));

    JobDetailImpl jobDetail = new JobDetailImpl();
    UUID jobId = UUID.randomUUID();
    jobDetail.setKey(new JobKey(jobId.toString(), "bar"));
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    new PfbQuartzJob(jobDao, wsmDao, restClientRetry).execute(mockContext);

    // Should not call wsm dao
    verify(wsmDao, times(0)).createDataRepoSnapshotReference(any());
    // But job should succeed
    verify(jobDao).succeeded(jobId);
  }

  @Test
  void snapshotIdsAreParsed() throws JobExecutionException {
    JobExecutionContext mockContext = mock(JobExecutionContext.class);
    URL resourceUrl = getClass().getResource("/test.avro");
    when(mockContext.getMergedJobDataMap())
        .thenReturn(
            new JobDataMap(Map.of(ARG_TOKEN, "expectedToken", ARG_URL, resourceUrl.toString())));

    JobDetailImpl jobDetail = new JobDetailImpl();
    UUID jobId = UUID.randomUUID();
    jobDetail.setKey(new JobKey(jobId.toString(), "bar"));
    when(mockContext.getJobDetail()).thenReturn(jobDetail);

    new PfbQuartzJob(jobDao, wsmDao, restClientRetry).execute(mockContext);

    // This is the snapshotId given in the test pfb
    verify(wsmDao)
        .createDataRepoSnapshotReference(
            ArgumentMatchers.argThat(
                new SnapshotModelMatcher(UUID.fromString("790795c4-49b1-4ac8-a060-207b92ea08c5"))));
    // Job should succeed
    verify(jobDao).succeeded(jobId);
  }

  @ParameterizedTest(name = "paginates through results when WSM has {0} references")
  @ValueSource(ints = {0, 1, 49, 50, 51, 99, 100, 101, 456})
  void paginateExistingSnapshots(int wsmCount) throws ApiException {
    int testPageSize = 50; // page size to use during this test

    List<ResourceDescription> mockResources = new ArrayList<>();
    // generate the full list of snapshots as known by our mock WSM
    for (int i = 0; i < wsmCount; i++) {
      ResourceDescription resourceDescription = new ResourceDescription();
      ResourceAttributesUnion resourceAttributesUnion = new ResourceAttributesUnion();
      DataRepoSnapshotAttributes dataRepoSnapshotAttributes = new DataRepoSnapshotAttributes();
      dataRepoSnapshotAttributes.setSnapshot(UUID.randomUUID().toString());
      dataRepoSnapshotAttributes.setInstanceName("index: " + i);
      resourceAttributesUnion.setGcpDataRepoSnapshot(dataRepoSnapshotAttributes);
      resourceDescription.setResourceAttributes(resourceAttributesUnion);
      mockResources.add(resourceDescription);
    }
    // configure the mock to return the appropriate page of snapshots
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenAnswer(
            invocation -> {
              int offset = invocation.getArgument(1);
              int limit = invocation.getArgument(2);
              int sliceEnd =
                  Math.min(offset + limit, mockResources.size()); // slice may be a partial page
              // calculate the slice to return
              List<ResourceDescription> slice = mockResources.subList(offset, sliceEnd);
              ResourceList resourceList = new ResourceList();
              resourceList.setResources(slice);
              return resourceList;
            });

    PfbQuartzJob pfbQuartzJob = new PfbQuartzJob(jobDao, wsmDao, restClientRetry);
    ResourceList actual = pfbQuartzJob.listAllSnapshots(testPageSize);

    // assert total size of all results
    assertEquals(wsmCount, actual.getResources().size());
    // assert that the "snapshot instance name" (not "WDS instance") is unique in all our results
    // i.e. we did not return the same snapshot more than once
    List<String> snapshotInstanceNames =
        actual.getResources().stream()
            .map(res -> res.getResourceAttributes().getGcpDataRepoSnapshot().getInstanceName())
            .distinct()
            .toList();
    assertEquals(wsmCount, snapshotInstanceNames.size());
    // assert the number of requests made to WSM to generate the list
    double expectedInvocations = Math.floor((double) wsmCount / testPageSize) + 1;
    verify(wsmDao, times((int) expectedInvocations))
        .enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt());
  }

  @Test
  void safeGetSnapshotId() {
    UUID snapshotId = UUID.randomUUID();
    ResourceDescription resourceDescription = createResourceDescription(snapshotId);

    PfbQuartzJob pfbQuartzJob = new PfbQuartzJob(jobDao, wsmDao, restClientRetry);
    UUID actual = pfbQuartzJob.safeGetSnapshotId(resourceDescription);

    assertEquals(snapshotId, actual);
  }

  @Test
  void safeGetSnapshotIdNonUuid() {
    String notAUuid = "Hello world";

    DataRepoSnapshotAttributes dataRepoSnapshotAttributes = new DataRepoSnapshotAttributes();
    dataRepoSnapshotAttributes.setSnapshot(notAUuid);

    ResourceAttributesUnion resourceAttributes = new ResourceAttributesUnion();
    resourceAttributes.setGcpDataRepoSnapshot(dataRepoSnapshotAttributes);

    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(resourceAttributes);

    PfbQuartzJob pfbQuartzJob = new PfbQuartzJob(jobDao, wsmDao, restClientRetry);
    UUID actual = pfbQuartzJob.safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void safeGetSnapshotIdNull() {
    String notAUuid = null;

    DataRepoSnapshotAttributes dataRepoSnapshotAttributes = new DataRepoSnapshotAttributes();
    dataRepoSnapshotAttributes.setSnapshot(notAUuid);

    ResourceAttributesUnion resourceAttributes = new ResourceAttributesUnion();
    resourceAttributes.setGcpDataRepoSnapshot(dataRepoSnapshotAttributes);

    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(resourceAttributes);

    PfbQuartzJob pfbQuartzJob = new PfbQuartzJob(jobDao, wsmDao, restClientRetry);
    UUID actual = pfbQuartzJob.safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void safeGetSnapshotIdNoSnapshotObject() {
    ResourceAttributesUnion resourceAttributes = new ResourceAttributesUnion();
    resourceAttributes.setGcpDataRepoSnapshot(null);

    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(resourceAttributes);

    PfbQuartzJob pfbQuartzJob = new PfbQuartzJob(jobDao, wsmDao, restClientRetry);
    UUID actual = pfbQuartzJob.safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void safeGetSnapshotIdNoAttributes() {
    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(null);

    PfbQuartzJob pfbQuartzJob = new PfbQuartzJob(jobDao, wsmDao, restClientRetry);
    UUID actual = pfbQuartzJob.safeGetSnapshotId(resourceDescription);

    assertNull(actual);
  }

  @Test
  void existingPolicySnapshotIds() {
    List<UUID> expected = IntStream.range(0, 75).mapToObj(i -> UUID.randomUUID()).toList();

    List<ResourceDescription> resourceDescriptions =
        expected.stream().map(this::createResourceDescription).toList();

    ResourceList resourceList = new ResourceList();
    resourceList.setResources(resourceDescriptions);

    PfbQuartzJob pfbQuartzJob = new PfbQuartzJob(jobDao, wsmDao, restClientRetry);
    List<UUID> actual = pfbQuartzJob.existingPolicySnapshotIds(resourceList);

    assertEquals(expected, actual);
  }

  private ResourceDescription createResourceDescription(UUID snapshotId) {
    DataRepoSnapshotAttributes dataRepoSnapshotAttributes = new DataRepoSnapshotAttributes();
    dataRepoSnapshotAttributes.setSnapshot(snapshotId.toString());

    ResourceAttributesUnion resourceAttributes = new ResourceAttributesUnion();
    resourceAttributes.setGcpDataRepoSnapshot(dataRepoSnapshotAttributes);

    ResourceDescription resourceDescription = new ResourceDescription();
    resourceDescription.setResourceAttributes(resourceAttributes);

    return resourceDescription;
  }

  private class SnapshotModelMatcher implements ArgumentMatcher<SnapshotModel> {
    private final UUID expectedSnapshotId;

    public SnapshotModelMatcher(UUID expectedSnapshotId) {
      this.expectedSnapshotId = expectedSnapshotId;
    }

    @Override
    public boolean matches(SnapshotModel exampleSnapshot) {
      return exampleSnapshot.getId().equals(expectedSnapshotId);
    }
  }
}
