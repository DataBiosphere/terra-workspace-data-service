package org.databiosphere.workspacedataservice.controller;

import java.util.Map;
import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.info.InfoEndpoint;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for /status and /version endpoints. Returns the health and info objects managed by
 * Actuator. Since Actuator runs on port 9098, we need these minimal APIs to serve /status and
 * /health on the standard port.
 */
@RestController
public class InfoController {

  private final HealthEndpoint healthEndpoint;
  private final InfoEndpoint infoEndpoint;

  public InfoController(HealthEndpoint healthEndpoint, InfoEndpoint infoEndpoint) {
    this.healthEndpoint = healthEndpoint;
    this.infoEndpoint = infoEndpoint;
  }

  @GetMapping("/status")
  public HealthComponent status() {
    return healthEndpoint.health();
  }

  @GetMapping("/version")
  public Map<String, Object> version() {
    return infoEndpoint.info();
  }
}
