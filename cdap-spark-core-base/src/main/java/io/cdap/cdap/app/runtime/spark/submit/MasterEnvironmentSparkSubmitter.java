/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.app.runtime.spark.submit;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeEnv;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils;
import io.cdap.cdap.app.runtime.spark.distributed.SparkExecutionService;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.spark.SparkConfig;
import io.cdap.cdap.master.spi.environment.spark.SparkLocalizeResource;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Master environment spark submitter.
 */
public class MasterEnvironmentSparkSubmitter extends AbstractSparkSubmitter {
  private static final Logger LOG = LoggerFactory.getLogger(MasterEnvironmentSparkSubmitter.class);
  private final SparkExecutionService sparkExecutionService;
  private final MasterEnvironment masterEnv;
  private SparkConfig sparkConfig;
  private List<LocalizeResource> resources;

  /**
   * Master environment spark submitter constructor.
   */
  public MasterEnvironmentSparkSubmitter(LocationFactory locationFactory, String hostname,
                                         SparkRuntimeContext runtimeContext, MasterEnvironment masterEnv) {
    ProgramRunId programRunId = runtimeContext.getProgram().getId().run(runtimeContext.getRunId().getId());
    WorkflowProgramInfo workflowInfo = runtimeContext.getWorkflowInfo();
    BasicWorkflowToken workflowToken = workflowInfo == null ? null : workflowInfo.getWorkflowToken();
    this.sparkExecutionService = new SparkExecutionService(locationFactory, hostname, programRunId, workflowToken);
    this.masterEnv = masterEnv;
  }

  @Override
  protected Iterable<LocalizeResource> getFiles(List<LocalizeResource> localizeResources) {
    this.resources = Collections.unmodifiableList(new ArrayList<>(localizeResources));
    return Collections.emptyList();
  }

  @Override
  protected Map<String, String> getSubmitConf() {
    Map<String, String> config = new HashMap<>();
    config.put(SparkConfig.DRIVER_ENV_PREFIX + "CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    config.put("spark.executorEnv.CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    config.putAll(getSparkConfig().getConfigs());
    return config;
  }

  @Override
  protected void addMaster(Map<String, String> configs, ImmutableList.Builder<String> argBuilder) {
    argBuilder.add("--master").add(getSparkConfig().getMaster()).add("--deploy-mode").add("cluster");
  }

  @Override
  protected List<String> beforeSubmit() {
    sparkExecutionService.startAndWait();
//    SparkRuntimeEnv.setProperty(SparkConfig.DRIVER_ENV_PREFIX + SparkRuntimeUtils.CDAP_SPARK_EXECUTION_SERVICE_URI,
//                                sparkExecutionService.getBaseURI().toString());
    return Collections.emptyList();
  }

  @Override
  protected void triggerShutdown() {
    // Just stop the execution service and block on that.
    // It will wait until the "completed" call from the Spark driver.
    sparkExecutionService.stopAndWait();
  }

  @Override
  protected void onCompleted(boolean succeeded) {
    if (succeeded) {
      sparkExecutionService.stopAndWait();
    } else {
      sparkExecutionService.shutdownNow();
    }
  }

  private SparkConfig getSparkConfig() {
    if (sparkConfig == null) {
      sparkConfig = masterEnv.getSparkSubmitConfig(getLocalizeResources(resources));
    }
    return sparkConfig;
  }

  private Map<String, SparkLocalizeResource> getLocalizeResources(List<LocalizeResource> resources) {
    Map<String, SparkLocalizeResource> map = new HashMap<>();
    for (LocalizeResource resource : resources) {
      map.put(FilenameUtils.getName(resource.getURI().toString()), new SparkLocalizeResource(resource.getURI()));
    }
    return map;
  }
}
