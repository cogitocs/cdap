package io.cdap.cdap.internal.tether;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

public class TetherServerModule extends AbstractModule {
  @Override
  public void configure() {
    bind(TetherServerHandler.class).in(Scopes.SINGLETON);
  }
}
