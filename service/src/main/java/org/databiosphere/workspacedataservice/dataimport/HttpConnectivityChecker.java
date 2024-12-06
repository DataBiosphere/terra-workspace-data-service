package org.databiosphere.workspacedataservice.dataimport;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Objects;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

@Configuration
public class HttpConnectivityChecker implements ConnectivityChecker {

  private final Logger logger = LoggerFactory.getLogger(HttpConnectivityChecker.class);

  private final DataImportProperties dataImportProperties;

  public HttpConnectivityChecker(DataImportProperties dataImportProperties) {
    this.dataImportProperties = dataImportProperties;
  }

  public boolean validateConnectivity(URI importUrl) throws IOException {
    // short-circuit if not enabled
    if (!dataImportProperties.isConnectivityCheckEnabled()) {
      return true;
    }

    // we only validate connectivity for https urls for now
    if (!Objects.equals(importUrl.getScheme(), "https")) {
      return true;
    }

    logger.info("Checking connectivity to import URI ...");

    // set up the connection to the url-to-be-imported
    // we may want to make the timeouts configurable at some point
    URL urlObject = importUrl.toURL();
    HttpURLConnection conn = (HttpURLConnection) urlObject.openConnection();
    conn.setRequestMethod("HEAD");
    conn.setConnectTimeout(5000);
    conn.setReadTimeout(5000);

    // perform the connection and get the response code
    int code = conn.getResponseCode();
    HttpStatusCode statusCode = HttpStatusCode.valueOf(code);

    logger.info("Import URI responded with {}", statusCode);

    // success?
    if (statusCode.is2xxSuccessful()) {
      return true;
    }

    // if not success, throw an appropriate error
    if (statusCode.is4xxClientError()) {
      throw new HttpClientErrorException(statusCode);
    } else if (statusCode.is5xxServerError()) {
      throw new HttpServerErrorException(statusCode);
    } else {
      throw new ValidationException("URI to be imported returned status code " + code);
    }
  }
}
