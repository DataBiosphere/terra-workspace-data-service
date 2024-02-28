package org.databiosphere.workspacedataservice.sam;

import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

/** Holds the BearerToken for the current request. */
public class BearerTokenHolder {

  @Nullable private BearerToken token;

  /** Returns the BearerToken if present, otherwise returns {@code BearerToken.empty()}. */
  @NonNull
  public BearerToken getToken() {
    if (token == null) {
      return BearerToken.empty();
    }
    return token;
  }

  void setToken(BearerToken token) {
    this.token = token;
  }
}
