package org.databiosphere.workspacedataservice.drshub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpStatusCodeException;

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
  void resolveDrs_ReturnsExpectedResponse() {
    // ARRANGE
    String drsUri = "drs://jade.datarepo-test.broadinstitute.org/123";
    ResourceMetadataResponse.AccessUrl accessUrl =
        new ResourceMetadataResponse.AccessUrl(
            URI.create("https://example.com"), Map.of("header1", "value1"));
    ResourceMetadataResponse expectedResponse = new ResourceMetadataResponse(accessUrl);
    ResolveDrsRequest resolveDrsRequest = new ResolveDrsRequest(drsUri, List.of("accessUrl"));

    when(mockDrsHubApi.resolveDrs(resolveDrsRequestCaptor.capture())).thenReturn(expectedResponse);

    // ACT
    ResourceMetadataResponse response = drsHubClient.resolveDrs(resolveDrsRequest);

    // ASSERT
    verify(mockDrsHubApi).resolveDrs(resolveDrsRequestCaptor.capture());
    assertEquals(drsUri, resolveDrsRequestCaptor.getValue().url());
    assertEquals(expectedResponse, response);
  }

  @Test
  void resolveDrs_ThrowsNotFound() {
    // ARRANGE
    String drsUri = "drs://jade.datarepo-test.broadinstitute.org/unknown";
    ResolveDrsRequest resolveDrsRequest = new ResolveDrsRequest(drsUri, List.of("accessUrl"));
    HttpStatusCodeException exception = new HttpClientErrorException(HttpStatus.NOT_FOUND);

    when(mockDrsHubApi.resolveDrs(resolveDrsRequest)).thenThrow(exception);

    // ACT
    RestException thrown =
        assertThrows(RestException.class, () -> drsHubClient.resolveDrs(resolveDrsRequest));

    // ASSERT
    assertEquals(HttpStatus.NOT_FOUND, thrown.getStatusCode());
    assertTrue(thrown.getMessage().contains("404 NOT_FOUND"));
  }

  @Test
  void resolveDrs_ThrowsTimeout() {
    // ARRANGE
    String drsUri = "drs://jade.datarepo-test.broadinstitute.org/timeout";
    ResolveDrsRequest resolveDrsRequest = new ResolveDrsRequest(drsUri, List.of("accessUrl"));
    HttpStatusCodeException exception = new HttpClientErrorException(HttpStatus.GATEWAY_TIMEOUT);

    when(mockDrsHubApi.resolveDrs(resolveDrsRequest)).thenThrow(exception);

    // ACT
    RestException thrown =
        assertThrows(RestException.class, () -> drsHubClient.resolveDrs(resolveDrsRequest));

    // ASSERT
    assertEquals(HttpStatus.GATEWAY_TIMEOUT, thrown.getStatusCode());
    assertTrue(thrown.getMessage().contains("504 GATEWAY_TIMEOUT"));
  }

  @Test
  void resolveDrs_ThrowsInternalServerError() {
    // ARRANGE
    String drsUri = "drs://jade.datarepo-test.broadinstitute.org/error";
    ResolveDrsRequest resolveDrsRequest = new ResolveDrsRequest(drsUri, List.of("accessUrl"));
    HttpStatusCodeException exception =
        new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR);

    when(mockDrsHubApi.resolveDrs(resolveDrsRequest)).thenThrow(exception);

    // ACT
    RestException thrown =
        assertThrows(RestException.class, () -> drsHubClient.resolveDrs(resolveDrsRequest));

    // ASSERT
    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, thrown.getStatusCode());
    assertTrue(thrown.getMessage().contains("500 INTERNAL_SERVER_ERROR"));
  }
}
