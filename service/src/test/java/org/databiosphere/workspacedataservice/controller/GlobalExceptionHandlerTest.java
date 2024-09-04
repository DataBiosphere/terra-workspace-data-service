package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class GlobalExceptionHandlerTest extends DataPlaneTestBase {
  @Autowired private GlobalExceptionHandler globalExceptionHandler;

  @Test
  void gatherNestedErrorMessagesNullMessage() {
    Throwable input = new Throwable();
    List<String> actual =
        globalExceptionHandler.gatherNestedErrorMessages(input, new ArrayList<>());
    assertThat(actual).isEmpty();
  }

  @Test
  void gatherNestedErrorMessagesEmptyMessage() {
    Throwable input = new Throwable("");
    List<String> actual =
        globalExceptionHandler.gatherNestedErrorMessages(input, new ArrayList<>());
    assertThat(actual).isEmpty();
  }

  @Test
  void gatherNestedErrorMessagesBlankMessage() {
    Throwable input = new Throwable(" ");
    List<String> actual =
        globalExceptionHandler.gatherNestedErrorMessages(input, new ArrayList<>());
    assertThat(actual).isEmpty();
  }

  @Test
  void gatherNestedErrorMessagesOneMessage() {
    Throwable input = new Throwable("one");
    List<String> actual =
        globalExceptionHandler.gatherNestedErrorMessages(input, new ArrayList<>());
    assertThat(actual).size().isEqualTo(1);
    assertEquals("one", actual.get(0));
  }

  @Test
  void gatherNestedErrorMessagesTwoMessages() {
    Throwable input = new Throwable("one");
    Throwable nested = new Throwable("two");
    input.initCause(nested);
    List<String> actual =
        globalExceptionHandler.gatherNestedErrorMessages(input, new ArrayList<>());
    assertThat(actual).size().isEqualTo(2);
    assertEquals("one", actual.get(0));
    assertEquals("two", actual.get(1));
  }

  @Test
  void gatherNestedErrorMessagesThreeMessages() {
    Throwable input = new Throwable("one");
    Throwable nested = new Throwable("two");
    Throwable third = new Throwable("three");
    nested.initCause(third);
    input.initCause(nested);
    List<String> actual =
        globalExceptionHandler.gatherNestedErrorMessages(input, new ArrayList<>());
    assertThat(actual).size().isEqualTo(3);
    assertEquals("one", actual.get(0));
    assertEquals("two", actual.get(1));
    assertEquals("three", actual.get(2));
  }
}
