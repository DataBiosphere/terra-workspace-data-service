package org.databiosphere.workspacedataservice.storage;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.nio.charset.Charset.defaultCharset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.util.Streams.stream;
import static org.springframework.util.StreamUtils.copyToString;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@Component
@DirtiesContext
@SpringBootTest
// inheritProfiles = false to make sure data-plane is not active from DataPlaneTestBase
@ActiveProfiles(value = "control-plane", inheritProfiles = false)
class GcsStorageTest extends DataPlaneTestBase {
  @Qualifier("mockGcsStorage")
  @Autowired
  private GcsStorage storage;

  @AfterEach
  void cleanup() {
    storage.getBlobsInBucket().forEach(Blob::delete);
  }

  @Test
  void createAndGetBlobSimple() throws IOException {
    // Arrange
    String initialString = "text";

    // Act
    try (OutputStream outputStream = storage.getOutputStream("testBlobName")) {
      outputStream.write(initialString.getBytes());
    }
    String actualContent = getContentsAsString("testBlobName");

    // Assert
    assertThat(actualContent).isEqualTo(initialString);
  }

  @Test
  void createAndGetBlobEmpty() throws IOException {
    // Act
    storage.createBlob("emptyBlob");
    String actualContent = getContentsAsString("emptyBlob");

    // Assert
    var onlyBlob = assertSingleBlob();
    assertThat(onlyBlob.getName()).isEqualTo("emptyBlob");
    assertThat(actualContent).isEmpty();
  }

  @Test
  void getBlobContentsMissing() {
    // Act / Assert
    assertThatExceptionOfType(IOException.class)
        .isThrownBy(() -> storage.getBlobContents("missingBlob"));
  }

  @Test
  void getBlobsInBucketEmpty() throws IOException {
    // Act / Assert
    assertThat(storage.getBlobsInBucket()).isEmpty();
  }

  @Test
  void getBlobsInBucketSingle() throws IOException {
    // Arrange
    try (OutputStream outputStream = storage.getOutputStream("testBlobName")) {
      outputStream.write("text".getBytes());
    }

    // Act / Assert
    Blob onlyBlob = assertSingleBlob();

    // Assert
    assertThat(onlyBlob.asBlobInfo())
        .hasFieldOrPropertyWithValue("bucket", storage.getBucketName())
        .hasFieldOrPropertyWithValue("name", "testBlobName");
  }

  @Test
  void getBlobsInBucketMultiple() throws IOException {
    // Arrange
    try (OutputStream outputStream = storage.getOutputStream("testBlobName1")) {
      outputStream.write("text".getBytes());
    }

    try (OutputStream outputStream = storage.getOutputStream("testBlobName2")) {
      outputStream.write("text".getBytes());
    }

    storage.createBlob("testBlobName3");

    // Act
    Iterable<Blob> blobsInBucket = storage.getBlobsInBucket();

    // Assert
    assertThat(blobsInBucket).hasSize(3);
    assertThat(stream(blobsInBucket).map(Blob::getName))
        .containsExactlyInAnyOrder("testBlobName1", "testBlobName2", "testBlobName3");
  }

  @Test
  void deleteBlob() throws IOException {
    // Arrange
    storage.createBlob("testBlobName");

    // Act
    storage.deleteBlob("testBlobName");

    // Assert
    assertThat(storage.getBlobsInBucket()).isEmpty();
  }

  @Test
  void deleteBlobWithContent() throws IOException {
    // Arrange
    storage.createBlob("testBlobName");
    try (OutputStream outputStream = storage.getOutputStream("testBlobName")) {
      outputStream.write("text".getBytes());
    }

    // Act
    storage.deleteBlob("testBlobName");

    // Assert
    assertThat(storage.getBlobsInBucket()).isEmpty();
  }

  @Test
  void deleteBlobMissing() {
    // Act / Assert
    assertThatNoException().isThrownBy(() -> storage.deleteBlob("missingBlob"));
  }

  @Test
  void moveExistingBlob() throws IOException {
    // Arrange
    String initialContent = "initial content";
    String newBlobName = "newBlobName";
    Blob sourceBlob = storage.createBlob("sourceBlobName");
    try (OutputStream outputStream = storage.getOutputStream(sourceBlob)) {
      outputStream.write(initialContent.getBytes());
    }

    // Act
    URI sourceUri = getUri(sourceBlob);
    Blob movedBlob = storage.moveBlob(sourceUri, BlobId.of(storage.getBucketName(), newBlobName));

    // Assert
    assertThat(movedBlob.getName()).isEqualTo(newBlobName);
    assertThat(getContentsAsString(newBlobName)).isEqualTo(initialContent);
    assertThatExceptionOfType(IOException.class)
        .isThrownBy(() -> storage.getBlobContents("sourceBlobName"))
        .withMessageContaining("The blob was not found");
  }

  @Test
  void moveNonExistentBlob() {
    // Arrange
    URI sourceUri = URI.create("gs://nonExistentBucket/nonExistentBlob");

    // Act / Assert
    assertThatExceptionOfType(StorageException.class)
        .isThrownBy(
            () -> storage.moveBlob(sourceUri, BlobId.of(storage.getBucketName(), "targetBlobName")))
        .withMessageContaining("nonExistent");
  }

  private String getContentsAsString(String blobName) throws IOException {
    return copyToString(storage.getBlobContents(blobName), defaultCharset());
  }

  private Blob assertSingleBlob() {
    Iterable<Blob> blobsInBucket = storage.getBlobsInBucket();
    assertThat(blobsInBucket).hasSize(1);
    return stream(blobsInBucket).collect(onlyElement());
  }

  private URI getUri(Blob blob) {
    return URI.create(blob.getBlobId().toGsUtilUri());
  }
}
