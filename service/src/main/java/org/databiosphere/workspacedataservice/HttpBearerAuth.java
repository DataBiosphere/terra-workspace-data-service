package org.databiosphere.workspacedataservice;

import java.net.http.HttpRequest;
import java.util.function.Consumer;

public class HttpBearerAuth implements Consumer<HttpRequest.Builder> {
    private final String bearerToken;

    public HttpBearerAuth(String bearerToken) {
        this.bearerToken = bearerToken;
    }

    @Override
    public void accept(HttpRequest.Builder builder) {
        if(bearerToken == null) {
            return;
        }
        builder.header("Authorization", "Bearer " + bearerToken);
    }

}