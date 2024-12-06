package org.databiosphere.workspacedataservice.dataimport;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Objects;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

@Configuration
public class HttpConnectivityChecker implements ConnectivityChecker {

  private final DataImportProperties dataImportProperties;

  public HttpConnectivityChecker(DataImportProperties dataImportProperties) {
    this.dataImportProperties = dataImportProperties;
  }

  public boolean validateConnectivity(URI importUrl) throws IOException {
    // short-circuit if not enabled
    if (!dataImportProperties.isConnectivityCheckEnabled()) {
      return true;
    }

    // we only validate connectivity for https:// urls;
    // we may want to validate gs:// urls at some future point
    if (!Objects.equals(importUrl.getScheme(), "https")) {
      return true;
    }

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
