package org.databiosphere.workspacedataservice.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.core.io.Resource;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SwaggerUiConfigTest {

  @Mock private Resource swaggerUiControlPlanePreviewResource;

  @Mock private Resource swaggerUiControlPlaneResource;

  @Mock private Resource swaggerUiDataPlaneResource;

  @InjectMocks private SwaggerUiConfig swaggerUiConfig;

  private final String previewControlPlaneHtml = "<html>Control Plane Preview</html>";
  private final String controlPlaneHtml = "<html>Control Plane</html>";
  private final String dataPlaneHtml = "<html>Data Plane</html>";

  @BeforeEach
  void setUp() throws IOException {
    when(swaggerUiControlPlanePreviewResource.getInputStream())
        .thenReturn(
            new ByteArrayInputStream(previewControlPlaneHtml.getBytes(StandardCharsets.UTF_8)));
    when(swaggerUiControlPlaneResource.getInputStream())
        .thenReturn(new ByteArrayInputStream(controlPlaneHtml.getBytes(StandardCharsets.UTF_8)));
    when(swaggerUiDataPlaneResource.getInputStream())
        .thenReturn(new ByteArrayInputStream(dataPlaneHtml.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void testControlPlaneSwaggerHtml_PreviewOn() throws IOException {
    String result =
        swaggerUiConfig.controlPlaneSwaggerHtml(
            "on", swaggerUiControlPlanePreviewResource, swaggerUiControlPlaneResource);
    assertEquals(previewControlPlaneHtml, result);
  }

  @Test
  void testControlPlaneSwaggerHtml_PreviewOff() throws IOException {
    String result =
        swaggerUiConfig.controlPlaneSwaggerHtml(
            "off", swaggerUiControlPlanePreviewResource, swaggerUiControlPlaneResource);
    assertEquals(controlPlaneHtml, result);
  }

  @Test
  void testDataPlaneSwaggerHtml() throws IOException {
    String result = swaggerUiConfig.dataPlaneSwaggerHtml(swaggerUiDataPlaneResource);
    assertEquals(dataPlaneHtml, result);
  }
}
