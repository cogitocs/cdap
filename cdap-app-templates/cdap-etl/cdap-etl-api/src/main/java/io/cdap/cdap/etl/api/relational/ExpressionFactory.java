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

import java.util.Set;

public interface ExpressionFactory {
  Set<Capability> getCapabilities();
  Expression compile(String expression);

  /**
   * Available with {@link CoreExpressionCapabilities#CAN_GET_QUALIFIED_DATASET_NAME}
   */
  default String getQualifiedDataSetName(Relation dataSet) {
    throw new UnsupportedOperationException();
  }

  /**
   * Available with {@link CoreExpressionCapabilities#CAN_GET_QUALIFIED_COLUMN_NAME}
   */
  default String getQualifiedColumnName(Relation dataSet, String column) {
    throw new UnsupportedOperationException();
  }

  /**
   * Available with {@link CoreExpressionCapabilities#CAN_SET_DATASET_ALIAS}
   */
  default Relation setDataSetAlias(Relation dataSet, String alias) {
    throw new UnsupportedOperationException();
  }
}
