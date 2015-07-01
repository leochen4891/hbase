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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;

public class MetricsStochasticBalancerSourceImpl extends MetricsBalancerSourceImpl implements
    MetricsStochasticBalancerSource {
  private static final String TABLE_FUNCTION_SEP = "_";
  private static final int MRU_SIZE = 1000;
  private static final float MRU_LOAD_FACTOR = 0.75f;
  private static final int MRU_CAPACITY = (int)Math.ceil(MRU_SIZE/MRU_LOAD_FACTOR) + 1;

  private Map<String, Map<String, Double>> stochasticCosts = null; 
  
  public MetricsStochasticBalancerSourceImpl() {
    stochasticCosts = Collections.synchronizedMap(
        new LinkedHashMap<String, Map<String, Double>>(MRU_CAPACITY, MRU_LOAD_FACTOR, true) {
          private static final long serialVersionUID = 8204713453436906599L;

          @Override
          protected boolean removeEldestEntry(Map.Entry<String, Map<String, Double>> eldest) {
            return size() > MRU_SIZE;
          }
        });
  }

  /**
   * The function that report stochastic load balancer costs to JMX
   */
  public void updateStochasticCost(String tableName, String costFunctionName,
      String costFunctionDesc, Double value) {
    if (tableName == null || costFunctionName == null) {
      return;
    }

    Map<String, Double> costs= stochasticCosts.get(tableName);
    if (costs== null) {
      costs= new ConcurrentHashMap<String, Double>();
    }

    costs.put(costFunctionName, value);
    stochasticCosts.put(tableName, costs);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder metricsRecordBuilder = metricsCollector.addRecord(metricsName);

    if (stochasticCosts != null) {
      for (String tableName : stochasticCosts.keySet()) {
        Map<String, Double> costs = stochasticCosts.get(tableName);
        for (String key : costs.keySet()) {
          double cost = costs.get(key);
          String attrName = tableName + ((tableName.length() <= 0) ? "" : TABLE_FUNCTION_SEP) + key;
          metricsRecordBuilder.addGauge(Interns.info(attrName, attrName), cost);
        }
      }
    }
    metricsRegistry.snapshot(metricsRecordBuilder, all);
  }
}
