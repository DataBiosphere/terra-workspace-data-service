package org.databiosphere.workspacedataservice.controller;

import java.util.Map;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.*;
import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.HttpCodeStatusMapper;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.actuate.info.InfoEndpoint;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for /status and /version endpoints. Returns the health and info objects managed by
 * Actuator. Since Actuator runs on port 9098, we need these minimal APIs to serve /status and
 * /health on the standard port.
 */
@DataPlane
@ControlPlane
@RestController
public class InfoController {

  private final HealthEndpoint healthEndpoint;
  private final InfoEndpoint infoEndpoint;
  private final HttpCodeStatusMapper statusMapper;

  public InfoController(
      HealthEndpoint healthEndpoint, InfoEndpoint infoEndpoint, HttpCodeStatusMapper statusMapper) {
    this.healthEndpoint = healthEndpoint;
    this.infoEndpoint = infoEndpoint;
    this.statusMapper = statusMapper;
  }

  @GetMapping("/status")
  public ResponseEntity<HealthComponent> status() {
    HealthComponent health = healthEndpoint.health();
    int statusCode = statusMapper.getStatusCode(health.getStatus());

    return ResponseEntity.status(statusCode).body(health);
  }

  @GetMapping("/status/liveness")
  public ResponseEntity<HealthComponent> statusLiveness() {
    HealthComponent health = healthEndpoint.healthForPath("liveness");
    int statusCode = statusMapper.getStatusCode(health.getStatus());

    return ResponseEntity.status(statusCode).body(health);
  }

  @GetMapping("/status/readiness")
  public ResponseEntity<Status> statusReadiness() {
    HealthComponent health = healthEndpoint.healthForPath("readiness");
    int statusCode = statusMapper.getStatusCode(health.getStatus());

    return ResponseEntity.status(statusCode).body(health.getStatus());
  }

  @GetMapping("/version")
  public Map<String, Object> version() {
    return infoEndpoint.info();
  }
}
