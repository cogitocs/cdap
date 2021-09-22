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

package io.cdap.cdap.etl.spark;

import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.RelationalTransform;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;

import java.util.Map;
import java.util.Optional;

/**
 * This interface defines an entity that can do relational tranform on {@link SparkCollection} with
 * {@link io.cdap.cdap.etl.api.relational.RelationalTransform}. Usually it holds a reference to the engine.
 */
public interface SparkRelationalEngine {
  Engine getRelationalEngine();
  <T> Optional<SparkCollection<T>> tryRelationalTransform(StageSpec stageSpec,
                                                      RelationalTransform transform,
                                                      Map<String, SparkCollection<Object>> input);
}
