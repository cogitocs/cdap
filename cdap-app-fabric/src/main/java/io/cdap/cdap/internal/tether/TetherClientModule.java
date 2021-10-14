package io.cdap.cdap.internal.tether;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

public class TetherClientModule extends AbstractModule {
  @Override
  public void configure() {
    bind(TetherClientHandler.class).in(Scopes.SINGLETON);
  }
}
