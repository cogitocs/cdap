/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.metrics.process;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import org.apache.twill.kafka.client.TopicPartition;

/**
 * An abstraction on persistent storage of kafka consumer information.
 */
public final class KafkaConsumerMetaTable extends AbstractConsumerMetaTable {

  public KafkaConsumerMetaTable(MetricsTable metaTable) {
    super(metaTable);
  }

  @Override
  protected byte[] getKey(Object topicPartitionObject) {
    TopicPartition topicPartition = TopicPartition.class.cast(topicPartitionObject);
    return Bytes.toBytes(String.format("%s.%02d", topicPartition.getTopic(), topicPartition.getPartition()));
  }
}
