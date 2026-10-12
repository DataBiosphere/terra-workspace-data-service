package org.databiosphere.workspacedataservice.service;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.databiosphere.workspacedataservice.dataimport.ImportValidator;
import org.databiosphere.workspacedataservice.generated.UrlFileInfoServerModel;
import org.databiosphere.workspacedataservice.generated.UrlInfoRequestServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SignedUrlUtilService {
  // matches the timeouts used by HttpConnectivityChecker
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final ImportValidator importValidator;
  private final HttpClient httpClient;

  public SignedUrlUtilService(ImportValidator importValidator) {
    this.importValidator = importValidator;
    // signed urls frequently redirect to the underlying storage object; follow those
    this.httpClient =
        HttpClient.newBuilder()
            .connectTimeout(TIMEOUT)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
  }

  private boolean isManifest(UrlInfoRequestServerModel signedUrl) {
    // TODO Save for later
    // checks if signed url is a manifest to other signed urls
    return false;
  }

  private List<URI> extractMultipleUrls(UrlInfoRequestServerModel urlInfoRequest) {
    HttpRequest request =
        HttpRequest.newBuilder().uri(urlInfoRequest.getUrl()).timeout(TIMEOUT).GET().build();

    HttpResponse<InputStream> response =
        send(request, HttpResponse.BodyHandlers.ofInputStream(), urlInfoRequest.getUrl());

    if (response.statusCode() != 200) {
      throw new ValidationException(
          "Manifest url returned status code %d.".formatted(response.statusCode()));
    }

    List<URI> uriList = new ArrayList<>();

    try (InputStream is = response.body()) {
      JSONObject jsonObject = new JSONObject(new JSONTokener(is));

      // TODO Update
      JSONArray jsonArray = jsonObject.getJSONArray("Avro Urls");

      for (int i = 0; i < jsonArray.length(); i++) {
        uriList.add(URI.create(jsonArray.getString(i)));
      }
    } catch (Exception e) {
      throw new ValidationException("Could not parse manifest: " + e.getMessage(), e);
    }

    return uriList;
  }

  private List<UrlFileInfoServerModel> getFileSizesFromManifestSignedUrl(List<URI> signedUris) {
    List<UrlFileInfoServerModel> fileInfos = new ArrayList<>();
    signedUris.forEach(
        uri -> {
          try {
            fileInfos.add(getFileSizeFromSignedUri(uri));
          } catch (Exception e) {
            // one unreachable file shouldn't fail the whole manifest; report it with an
            // unknown size so the caller can see which url could not be measured
            logger.error("Failed to fetch file size for URL: {}", uri, e);
            fileInfos.add(unknownSize(uri));
          }
        });
    return fileInfos;
  }

  private UrlFileInfoServerModel getFileSizeFromSignedUri(URI signedUri) {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(signedUri)
            .timeout(TIMEOUT)
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .build();

    HttpResponse<Void> response = send(request, HttpResponse.BodyHandlers.discarding(), signedUri);

    int statusCode = response.statusCode();
    if (statusCode < 200 || statusCode > 299) {
      // e.g. an expired signature returns 403 with no Content-Length; surface that rather
      // than silently reporting a size of -1
      throw new ValidationException("Url returned status code %d.".formatted(statusCode));
    }

    // -1 if the Content-Length header is missing
    Long size = response.headers().firstValueAsLong("Content-Length").orElse(-1L);

    UrlFileInfoServerModel info = new UrlFileInfoServerModel();
    info.setSignedUrl(signedUri.toString());
    info.setSizeBytes(size);
    return info;
  }

  private UrlFileInfoServerModel unknownSize(URI uri) {
    UrlFileInfoServerModel info = new UrlFileInfoServerModel();
    info.setSignedUrl(uri.toString());
    info.setSizeBytes(-1L);
    return info;
  }

  private <T> HttpResponse<T> send(
      HttpRequest request, HttpResponse.BodyHandler<T> bodyHandler, URI uri) {
    try {
      return httpClient.send(request, bodyHandler);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ValidationException("Interrupted while connecting to %s.".formatted(uri), e);
    } catch (Exception e) {
      throw new ValidationException(
          "Unable to connect to %s: %s".formatted(uri, e.getMessage()), e);
    }
  }

  public List<UrlFileInfoServerModel> getInfo(UrlInfoRequestServerModel urlInfoRequest) {
    importValidator.validateUrl(urlInfoRequest.getUrl());

    if (isManifest(urlInfoRequest)) {
      List<URI> uris = extractMultipleUrls(urlInfoRequest);
      return getFileSizesFromManifestSignedUrl(uris);
    } else {
      return List.of(getFileSizeFromSignedUri(urlInfoRequest.getUrl()));
    }
  }
}
