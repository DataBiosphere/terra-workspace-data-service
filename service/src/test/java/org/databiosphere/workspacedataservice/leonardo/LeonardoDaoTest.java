package org.databiosphere.workspacedataservice.leonardo;

import org.broadinstitute.dsde.workbench.client.leonardo.ApiException;
import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@DirtiesContext
@SpringBootTest(classes = {LeonardoConfig.class})
public class LeonardoDaoTest {
    @Autowired
    LeonardoDao leonardoDao;

    @MockBean
    LeonardoClientFactory leonardoClientFactory;

    final AppsV2Api mockAppsApi = Mockito.mock(AppsV2Api.class);

    @BeforeEach
    void beforeEach() {
        given(leonardoClientFactory.getAppsV2Api(any())).willReturn(mockAppsApi);
    }

    @Test
    void testWdsUrlNotReturned() throws ApiException {
        final int statusCode = HttpStatus.UNAUTHORIZED.value();
        given(mockAppsApi.listAppsV2(any(), any(), null, null))
                .willThrow(new org.broadinstitute.dsde.workbench.client.leonardo.ApiException(statusCode, "Intentional error thrown for unit test"));
        var exception = assertThrows(WorkspaceManagerException.class, () -> leonardoDao.getWdsEndpointUrl(any()));
        assertEquals(statusCode, exception.getRawStatusCode());
    }
}

