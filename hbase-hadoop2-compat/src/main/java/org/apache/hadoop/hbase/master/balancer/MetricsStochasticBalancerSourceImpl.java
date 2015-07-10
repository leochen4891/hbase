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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;

@InterfaceAudience.Private
public class MetricsStochasticBalancerSourceImpl extends MetricsBalancerSourceImpl implements
    MetricsStochasticBalancerSource {
  private static final String TABLE_FUNCTION_SEP = "_";

  // Use Most Recent Used(MRU) cache
  private static final float MRU_LOAD_FACTOR = 0.75f;
  private int metricsSize = 1000;
  private int mruCap = calcMruCap(metricsSize);

  private Map<String, Map<String, Double>> stochasticCosts = null;
  private Map<String, String> costFunctionDescs = null;

  private static final Log LOG = LogFactory.getLog(MetricsStochasticBalancerSourceImpl.class);

  public MetricsStochasticBalancerSourceImpl() {
    stochasticCosts =
        Collections.synchronizedMap(new LinkedHashMap<String, Map<String, Double>>(mruCap,
            MRU_LOAD_FACTOR, true) {
          private static final long serialVersionUID = 8204713453436906599L;

          @Override
          protected boolean removeEldestEntry(Map.Entry<String, Map<String, Double>> eldest) {
            return size() > mruCap;
          }
        });
    costFunctionDescs = new ConcurrentHashMap<String, String>();
  }
  
  /**
   *  Calculates the mru cache capacity from the metrics size
   */
  private static int calcMruCap(int metricsSize) {
    return (int) Math.ceil(metricsSize / MRU_LOAD_FACTOR) + 1;
  }
  
  @Override
  public void updateMetricsSize(int size) {
    if (size > 0) {
      metricsSize = size;
      mruCap = calcMruCap(size);
    } 
    // FIXME: TEST
    else {
      LOG.info("++++");
      LOG.info("++++ stochasticCostsSize = " + stochasticCosts.size() + ", metricsSize = " + metricsSize);
      for (Map.Entry<String, Map<String, Double>> t : stochasticCosts.entrySet()) {
        for (Map.Entry<String, Double> e : t.getValue().entrySet() ) {
          LOG.info(" ++++ " + t.getKey() + " - " + e.getKey() + " : " + e.getValue());
        }
      }
    }
  }

  /**
   * Reports stochastic load balancer costs to JMX
   */
  public void updateStochasticCost(String tableName, String costFunctionName, String functionDesc,
      Double cost) {
    if (tableName == null || costFunctionName == null || cost == null) {
      return;
    }

    if (functionDesc != null) {
      costFunctionDescs.put(costFunctionName, functionDesc);
    }

    Map<String, Double> costs = stochasticCosts.get(tableName);
    if (costs == null) {
      costs = new ConcurrentHashMap<String, Double>();
    }

    costs.put(costFunctionName, cost);
    stochasticCosts.put(tableName, costs);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder metricsRecordBuilder = metricsCollector.addRecord(metricsName);

    if (stochasticCosts != null) {
      for (String tableName : stochasticCosts.keySet()) {
        Map<String, Double> costs = stochasticCosts.get(tableName);
        for (String costFunctionName : costs.keySet()) {
          Double cost = costs.get(costFunctionName);
          String attrName = tableName + TABLE_FUNCTION_SEP + costFunctionName;
          String functionDesc = costFunctionDescs.get(costFunctionName);
          if (functionDesc == null) functionDesc = costFunctionName;
          metricsRecordBuilder.addGauge(Interns.info(attrName, functionDesc), cost);
        }
      }
    }
    metricsRegistry.snapshot(metricsRecordBuilder, all);
  }

}
