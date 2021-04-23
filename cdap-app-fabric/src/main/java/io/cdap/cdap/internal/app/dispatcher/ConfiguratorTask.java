/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.dispatcher;

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.InMemoryConfigurator;
import io.cdap.cdap.internal.app.deploy.pipeline.ConfiguratorConfig;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

/**
 * ConfiguratorTask is a {@link RunnableTask} for performing the config task in worker pod.
 */
public class ConfiguratorTask extends RunnableTask {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Logger LOG = LoggerFactory.getLogger(ConfiguratorTask.class);
  private final Impersonator impersonator;
  private final PluginFinder pluginFinder;
  private final ArtifactRepository artifactRepository;
  private final LocationFactory locationFactory;

  @Inject
  public ConfiguratorTask(Impersonator impersonator, PluginFinder pluginFinder,
                          ArtifactRepository artifactRepository, LocationFactory locationFactory) {
    this.impersonator = impersonator;
    this.pluginFinder = pluginFinder;
    this.artifactRepository = artifactRepository;
    this.locationFactory = locationFactory;
  }

  @Override
  protected byte[] run(String param) throws Exception {
    LOG.error("Got Injections: " + pluginFinder.toString() + " " + artifactRepository.toString());
    LOG.error("Recieved params: ");
    ConfiguratorConfig config = GSON.fromJson(param, ConfiguratorConfig.class);
    LOG.error("Successfully parsed the GSON: ");
    EntityImpersonator classLoaderImpersonator =
      new EntityImpersonator(config.getArtifactId().toEntityId(), impersonator);
    LOG.error("Created Impersoantor: ");
    Location artifactLocation = Locations.getLocationFromAbsolutePath(
      locationFactory, config.getArtifactLocationURI().getPath());
    OutputStream outputStream = artifactLocation.getOutputStream();
    LOG.error(String.format(
      "Artifact name, version: %s, %s", config.getArtifactId().getName(), config.getArtifactId().getVersion()));
    LOG.error(String.format(
      "Application name, version, classname: %s, %s, %s",
      config.getApplicationName(),
      config.getApplicationVersion(),
      config.getAppClassName()));
    InputStream artifactBytes = artifactRepository.getArtifactBytes(config.getArtifactId());
    ByteStreams.copy(artifactBytes, outputStream);
    outputStream.close();
    artifactBytes.close();
    ClassLoader artifactClassLoader = artifactRepository.createArtifactClassLoader(artifactLocation,
                                                                                   classLoaderImpersonator);
    LOG.error("Created class loader: ");
    InMemoryConfigurator configurator = new InMemoryConfigurator(
      config.getcConf(), config.getAppNamespace(), config.getArtifactId(),
      config.getAppClassName(), pluginFinder,
      artifactClassLoader,
      config.getApplicationName(), config.getApplicationVersion(),
      config.getConfigString());
    LOG.error("Created confiugrator ");
//

//
//    InMemoryConfigurator memoryConfigurator = new InMemoryConfigurator(config);
    LOG.error("Running config");
    ListenableFuture<ConfigResponse> future = configurator.config();
    LOG.error("Got future: " + future.toString());


    return toBytes(future.get(120, TimeUnit.SECONDS));
  }

  private byte[] toBytes(ConfigResponse obj) {
    String json = GSON.toJson(obj);
    return json.getBytes();
  }

  @Override
  protected void startUp() throws Exception {

  }

  @Override
  protected void shutDown() throws Exception {

  }
}
