package org.databiosphere.workspacedataservice.leonardo;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.text.SimpleDateFormat;
import java.util.*;
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException;
import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsApi;
import org.broadinstitute.dsde.workbench.client.leonardo.model.AppStatus;
import org.broadinstitute.dsde.workbench.client.leonardo.model.AppType;
import org.broadinstitute.dsde.workbench.client.leonardo.model.AuditInfo;
import org.broadinstitute.dsde.workbench.client.leonardo.model.ListAppResponse;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@DirtiesContext
@SpringBootTest
class LeonardoDaoTest extends ControlPlaneTestBase {
  @Autowired LeonardoDao leonardoDao;

  @MockitoBean LeonardoClientFactory leonardoClientFactory;

  final AppsApi mockAppsApi = mock(AppsApi.class);

  @BeforeEach
  void setUp() {
    given(leonardoClientFactory.getAppsV2Api(any())).willReturn(mockAppsApi);
  }

  final String expectedUrl =
      "https://lz28e3b6da2a7f727cae07c307f6c7c11d96da0d1fde444216.servicebus.windows.net/wds-e433ab84-1725-4666-b07e-4ee7ca";

  @Test
  void testWdsUrlNotReturned() throws ApiException {
    final int statusCode = HttpStatus.INTERNAL_SERVER_ERROR.value();
    given(
            mockAppsApi.listAppsV2(
                anyString(), nullable(String.class), anyBoolean(), nullable(String.class)))
        .willThrow(
            new org.broadinstitute.dsde.workbench.client.leonardo.ApiException(
                statusCode, "Intentional error thrown for unit test"));
    var exception =
        assertThrows(LeonardoServiceException.class, () -> leonardoDao.getWdsEndpointUrl(any()));
    assertEquals(statusCode, exception.getStatusCode().value());
  }

  @Test
  void testWdsUrlReturned() {
    var url =
        buildAppResponseAndCallExtraction(generateListAppResponse("wds", AppStatus.RUNNING, 1));
    assertEquals(expectedUrl, url);
  }

  @Test
  void testWdsUrlNotFoundWrongStatus() {
    var url =
        buildAppResponseAndCallExtraction(generateListAppResponse("wds", AppStatus.DELETED, 1));
    assertNull(url);
  }

  @Test
  void testWdsUrlNotFoundWrongKey() {
    var url =
        buildAppResponseAndCallExtraction(generateListAppResponse("not-wds", AppStatus.RUNNING, 1));
    assertNull(url);
  }

  @Test
  void testWdsUrlMultiple() {
    // tests the case if there are 2 running wds apps
    var url =
        buildAppResponseAndCallExtraction(generateListAppResponse("wds", AppStatus.RUNNING, 2));
    assertEquals(expectedUrl, url);
  }

  String buildAppResponseAndCallExtraction(List<ListAppResponse> responses) {
    return leonardoDao.extractWdsUrl(responses);
  }

  List<ListAppResponse> generateListAppResponse(String wdsKey, AppStatus wdsStatus, int count) {
    List<ListAppResponse> responseList = new ArrayList<>();
    var url = expectedUrl;
    while (count != 0) {
      ListAppResponse response = mock(ListAppResponse.class);
      Map<String, String> proxyUrls = new HashMap<>();
      proxyUrls.put(wdsKey, url);
      when(response.getProxyUrls()).thenReturn(proxyUrls);
      when(response.getStatus()).thenReturn(wdsStatus);
      when(response.getAppType()).thenReturn(AppType.WDS);
      when(response.getAuditInfo()).thenReturn(mock(AuditInfo.class));
      when(response.getAuditInfo().getCreatedDate())
          .thenReturn(
              new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()));
      responseList.add(response);
      count--;

      // only one wds entry will have a valid url, so mark url to be an empty string for all but the
      // first loop through
      url = "";
    }

    return responseList;
  }
}
