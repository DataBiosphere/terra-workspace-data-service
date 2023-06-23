package org.databiosphere.workspacedataservice.leonardo;

import org.broadinstitute.dsde.workbench.client.leonardo.ApiException;
import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api;
import org.broadinstitute.dsde.workbench.client.leonardo.model.AppStatus;
import org.broadinstitute.dsde.workbench.client.leonardo.model.ListAppResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DirtiesContext
@SpringBootTest(classes = {LeonardoConfig.class})
@TestPropertySource(properties = {"twds.instance.workspace-id=90e1b179-9f83-4a6f-a8c2-db083df4cd03"})
class LeonardoDaoTest {
    @Autowired
    LeonardoDao leonardoDao;

    @MockBean
    LeonardoClientFactory leonardoClientFactory;

    final AppsV2Api mockAppsApi = mock(AppsV2Api.class);

    @BeforeEach
    void beforeEach() {
        given(leonardoClientFactory.getAppsV2Api(any())).willReturn(mockAppsApi);
    }

    final String expectedUrl = "https://lz28e3b6da2a7f727cae07c307f6c7c11d96da0d1fde444216.servicebus.windows.net/wds-e433ab84-1725-4666-b07e-4ee7ca";
    @Test
    void testWdsUrlNotReturned() throws ApiException {
        final int statusCode = HttpStatus.INTERNAL_SERVER_ERROR.value();
        given(mockAppsApi.listAppsV2(String.valueOf(UUID.randomUUID()), "things", null, null))
                .willThrow(new org.broadinstitute.dsde.workbench.client.leonardo.ApiException(statusCode, "Intentional error thrown for unit test"));
        var exception = assertThrows(LeonardoServiceException.class, () -> leonardoDao.getWdsEndpointUrl(any()));
        assertEquals(statusCode, exception.getRawStatusCode());
    }

    @Test
    void testWdsUrlReturned() throws ApiException {
        var url = helperMethod("wds", AppStatus.RUNNING);
        assertEquals(expectedUrl, url);
    }

    @Test
    void testWdsUrlNotFoundWrongStatus() throws ApiException {
        var url = helperMethod("wds", AppStatus.DELETED);
        assertEquals(null, url);
    }

    @Test
    void testWdsUrlNotFoundWrongKey() throws ApiException {
        var url = helperMethod("not-wds", AppStatus.RUNNING);
        assertEquals(null, url);
    }

    String helperMethod(String wdsKey, AppStatus wdsStatus){
        ListAppResponse response = mock(ListAppResponse.class);
        Map<String, String> proxyUrls = new HashMap<String, String>() ;
        proxyUrls.put(wdsKey, expectedUrl);
        when(response.getProxyUrls()).thenReturn(proxyUrls);
        when(response.getStatus()).thenReturn(wdsStatus);
        List<ListAppResponse> responseList = new ArrayList<>();
        responseList.add(response);
        var url = leonardoDao.extractWdsUrl(responseList);
        return url;
    }
}

