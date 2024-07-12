package org.databiosphere.workspacedataservice.security;

import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

/** AuthenticationProvider for use with Sam/Terra. */
public class SamAuthenticationProvider implements AuthenticationProvider {

  /**
   * If we support the given Authentication type, and the Authentication object is authenticated,
   * return it unchanged. This method is mostly a no-op, since the real authentication happens
   * inside Sam. See also the SamWorkspaceActionsFilter, which performs the call to Sam.
   *
   * @return The authentication object if Sam returned ok; null otherwise.
   */
  @Override
  public Authentication authenticate(Authentication authentication) throws AuthenticationException {
    if (supports(authentication.getClass()) && authentication.isAuthenticated()) {
      return authentication;
    }
    return null;
  }

  /**
   * Does this auth provider support the given Authentication type?
   *
   * @return True if some variety of SamAuthToken
   */
  @Override
  public boolean supports(Class<?> authentication) {
    return SamAuthToken.class.isAssignableFrom(authentication);
  }
}
