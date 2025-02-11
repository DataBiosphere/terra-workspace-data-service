package org.databiosphere.workspacedataservice.drshub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@DirtiesContext
@SpringBootTest
class DrsHubClientTest extends ControlPlaneTestBase {

  @MockitoBean DrsHubApi mockDrsHubApi;

  @Autowired DrsHubClient drsHubClient;

  @BeforeEach
  void beforeEach() {
    reset(mockDrsHubApi);
  }

  @Captor ArgumentCaptor<ResolveDrsRequest> resolveDrsRequestCaptor;

  @Test
  void resolveDrs() {
    // ARRANGE
    String drsUri = "drs://jade.datarepo-test.broadinstitute.org/123";
    ResourceMetadataResponse.AccessUrl accessUrl =
        new ResourceMetadataResponse.AccessUrl(
            URI.create("https://example.com"), Map.of("header1", "value1"));
    ResourceMetadataResponse expectedResponse = new ResourceMetadataResponse(accessUrl);
    when(mockDrsHubApi.resolveDrs(resolveDrsRequestCaptor.capture())).thenReturn(expectedResponse);

    // ACT
    ResolveDrsRequest resolveDrsRequest = new ResolveDrsRequest(drsUri, List.of("accessUrl"));
    ResourceMetadataResponse response = drsHubClient.resolveDrs(resolveDrsRequest);

    // ASSERT
    verify(mockDrsHubApi).resolveDrs(resolveDrsRequestCaptor.capture());
    assertEquals(drsUri, resolveDrsRequestCaptor.getValue().url());
    assertEquals(expectedResponse, response);
  }
}
