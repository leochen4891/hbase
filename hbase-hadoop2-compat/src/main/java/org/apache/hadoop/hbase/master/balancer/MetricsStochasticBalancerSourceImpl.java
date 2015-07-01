/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.master.balancer;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsStochasticBalancerSourceImpl extends MetricsBalancerSourceImpl implements
    MetricsStochasticBalancerSource {
  private static final Log LOG = LogFactory.getLog(MetricsBalancerSourceImpl.class);
  Map<String, Map<String, Double>> stochasticCosts =
      new ConcurrentHashMap<String, Map<String, Double>>();

  /**
   * The function that report stochastic load balancer costs to JMX
   */
  public void updateStochasticCost(String tableName, String costFunctionName,
      String costFunctionDesc, Double value) {
    if (tableName == null || costFunctionName == null) {
      return;
    }

    Map<String, Double> map = stochasticCosts.get(tableName);
    if (map == null) {
      map = new ConcurrentHashMap<String, Double>();
    }

    map.put(costFunctionName, value);
    stochasticCosts.put(tableName, map);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder metricsRecordBuilder = metricsCollector.addRecord(metricsName);

    if (stochasticCosts != null) {
      for (String tableName : stochasticCosts.keySet()) {
        Map<String, Double> costs = stochasticCosts.get(tableName);
        for (String key : costs.keySet()) {
          double cost = costs.get(key);
          String attrName = tableName + ((tableName.length() <= 0) ? "" : "_") + key;
          metricsRecordBuilder.addGauge(Interns.info(attrName, attrName), cost);
        }
      }
    }
    metricsRegistry.snapshot(metricsRecordBuilder, all);
  }
}
