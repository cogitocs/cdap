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

package io.cdap.cdap.etl.api.relational;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface Engine {
  Set<Capability> getCapabilities();
  List<ExpressionFactory> getExpressionFactories();

  /**
   * Example: getExpressionFactory(StandardSQLCapabilities.SQL)
   * @param neededCapabilities
   * @return
   */
  default Optional<ExpressionFactory> getExpressionFactory(
    Capability... neededCapabilities) {
    return getExpressionFactory(Arrays.asList(neededCapabilities));
  }

  default Optional<ExpressionFactory> getExpressionFactory(
    Collection<Capability> neededCapabilities) {
    return getExpressionFactories().stream()
      .filter(f -> f.getCapabilities().containsAll(neededCapabilities))
      .findFirst();
  }
}
