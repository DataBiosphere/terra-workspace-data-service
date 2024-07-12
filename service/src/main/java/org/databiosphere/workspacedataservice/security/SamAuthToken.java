package org.databiosphere.workspacedataservice.security;

import java.io.Serial;
import java.util.Collection;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;

/** Spring Security Authentication token implementation for use with Sam. */
public class SamAuthToken extends AbstractAuthenticationToken {

  @Serial private static final long serialVersionUID = 0L;

  /**
   * This constructor required by the superclass. See SamWorkspaceActionsFilter
   *
   * @param samActions the collection of Sam actions the calling user has on a target workspace
   */
  public SamAuthToken(Collection<? extends GrantedAuthority> samActions) {
    super(samActions);
  }

  /**
   * Unused by our implementation, so return null.
   *
   * @return null
   */
  @Override
  public Object getCredentials() {
    return null;
  }

  /**
   * Unused by our implementation, so return null.
   *
   * @return null
   */
  @Override
  public Object getPrincipal() {
    return null;
  }
}
