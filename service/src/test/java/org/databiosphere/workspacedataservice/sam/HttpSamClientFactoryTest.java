package org.databiosphere.workspacedataservice.sam;

import org.broadinstitute.dsde.workbench.client.sam.auth.OAuth;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class HttpSamClientFactoryTest {

    @Test
    void getStatusApiWorksOutsideOfRequest() {
        var factory = new HttpSamClientFactory("http://localhost/sam");
        var statusApi = factory.getStatusApi();
        assertThat(statusApi).isNotNull();

        var apiClient = statusApi.getApiClient();
        assertThat(apiClient).isNotNull();
        assertThat(apiClient.getBasePath()).isEqualTo("http://localhost/sam");

        var oauths = apiClient.getAuthentications().values().stream().filter(auth -> auth instanceof OAuth).toList();
        assertThat(oauths.isEmpty()).isFalse();

        oauths.forEach(auth -> assertThat(((OAuth) auth).getAccessToken()).isNull());
    }

    @Test
    void getStatusApiWorksWithinARequest() throws ServletException, IOException {
        var request = new MockHttpServletRequest();
        request.addHeader(HttpHeaders.AUTHORIZATION, "fancytoken");

        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain filterChain = new MockFilterChain();

        new BearerTokenFilter().doFilter(request, response, filterChain);

        var factory = new HttpSamClientFactory("http://localhost/sam");
        var statusApi = factory.getStatusApi();
        assertThat(statusApi).isNotNull();

        var apiClient = statusApi.getApiClient();
        assertThat(apiClient).isNotNull();
        assertThat(apiClient.getBasePath()).isEqualTo("http://localhost/sam");

        var oauths = apiClient.getAuthentications().values().stream().filter(auth -> auth instanceof OAuth).toList();
        assertThat(oauths.isEmpty()).isFalse();

        oauths.forEach(auth -> assertThat(((OAuth) auth).getAccessToken()).isEqualTo("fancytoken"));
    }
}
