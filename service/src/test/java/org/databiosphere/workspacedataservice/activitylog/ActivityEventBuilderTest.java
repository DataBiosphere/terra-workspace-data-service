package org.databiosphere.workspacedataservice.activitylog;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.sam.BearerTokenFilter;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest
@AutoConfigureMockMvc
@ExtendWith(OutputCaptureExtension.class)
class ActivityEventBuilderTest extends DataPlaneTestBase {

  @Autowired CollectionService collectionService;
  @Autowired TwdsProperties twdsProperties;

  @MockBean SamClientFactory mockSamClientFactory;

  final UsersApi mockUsersApi = Mockito.mock(UsersApi.class);
  final ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

  @BeforeEach
  void setUp() {
    given(mockSamClientFactory.getUsersApi(any(BearerToken.class))).willReturn(mockUsersApi);
    given(mockSamClientFactory.getResourcesApi()).willReturn(mockResourcesApi);
  }

  @Test
  void testTokenResolutionViaSam(CapturedOutput output) throws ApiException {
    // set up the Sam mocks
    UserStatusInfo userStatusInfo = new UserStatusInfo();
    userStatusInfo.userSubjectId("userid-for-unit-tests-hello!");
    when(mockUsersApi.getUserStatusInfo()).thenReturn(userStatusInfo);
    when(mockResourcesApi.resourcePermissionV2(any(), any(), any())).thenReturn(true);

    // ensure we have a token in the current request; else we'll get "anonymous" for our user
    RequestAttributes currentAttributes = RequestContextHolder.currentRequestAttributes();
    currentAttributes.setAttribute(
        BearerTokenFilter.ATTRIBUTE_NAME_TOKEN, "fakey-token", SCOPE_REQUEST);
    RequestContextHolder.setRequestAttributes(currentAttributes);

    // create a collection; this will trigger logging
    CollectionServerModel collectionServerModel =
        TestUtils.createCollection(collectionService, twdsProperties.workspaceId());
    UUID collectionId = collectionServerModel.getId();

    // did we log the
    assertThat(output.getOut())
        .contains(
            "user userid-for-unit-tests-hello! created 1 collection(s) with id(s) [%s]"
                .formatted(collectionId));
  }
}
