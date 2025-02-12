package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.drshub.DrsHubApi;
import org.databiosphere.workspacedataservice.drshub.ResolveDrsRequest;
import org.databiosphere.workspacedataservice.drshub.ResourceMetadataResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class DrsServiceTest extends ControlPlaneTestBase {

  @Autowired DrsService drsService;
  @MockitoBean DrsHubApi mockDrsHubApi;

  @Test
  void testIsDrsUri() {
    URI drsUri = URI.create("drs://example.com/file");
    URI httpUri = URI.create("https://example.com/file");

    assertTrue(drsService.isDrsUri(drsUri));
    assertFalse(drsService.isDrsUri(httpUri));
  }

  @Test
  void testResolveDrsUri() {
    URI drsUri = URI.create("drs://example.com/file");
    URI resolvedUri = URI.create("https://example.com/file");

    ResourceMetadataResponse.AccessUrl accessUrl =
        new ResourceMetadataResponse.AccessUrl(resolvedUri, Map.of("header1", "value1"));
    ResolveDrsRequest drsRequest = new ResolveDrsRequest(drsUri.toString(), List.of("accessUrl"));
    ResourceMetadataResponse response = new ResourceMetadataResponse(accessUrl);

    when(mockDrsHubApi.resolveDrs(drsRequest)).thenReturn(response);

    URI result = drsService.resolveDrsUri(drsUri);
    assertEquals(resolvedUri, result);
  }

  @Test
  void testResolveDrsUriThrowsException() {
    URI drsUri = URI.create("drs://example.com/file");

    when(mockDrsHubApi.resolveDrs(new ResolveDrsRequest(drsUri.toString(), List.of("accessUrl"))))
        .thenThrow(new RuntimeException("Resolution failed"));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> drsService.resolveDrsUri(drsUri));

    assertEquals("Could not resolve DRS URI: Resolution failed", exception.getMessage());
  }
}
