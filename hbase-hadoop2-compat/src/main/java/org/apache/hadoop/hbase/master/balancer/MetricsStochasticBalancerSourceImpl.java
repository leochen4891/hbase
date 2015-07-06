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
import java.util.LinkedList;
import java.util.List;
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
  
  // TODO this MRU_SIZE is the limit of how many metrics can be reported for stochastic load balancer
  // It is hard-coded right now, and may be better if is configurable
  private static final int MRU_SIZE = 1000;
  private static final float MRU_LOAD_FACTOR = 0.75f;
  private static final int MRU_CAPACITY = (int)Math.ceil(MRU_SIZE/MRU_LOAD_FACTOR) + 1;
  private static final Log LOG = LogFactory.getLog(MetricsStochasticBalancerSourceImpl.class);

  private Map<String, Map<String, List<Object>>> stochasticCosts = null; 
  
  public MetricsStochasticBalancerSourceImpl() {
    stochasticCosts = Collections.synchronizedMap(
        new LinkedHashMap<String, Map<String, List<Object>>>(MRU_CAPACITY, MRU_LOAD_FACTOR, true) {
          private static final long serialVersionUID = 8204713453436906599L;

          @Override
          protected boolean removeEldestEntry(Map.Entry<String, Map<String, List<Object>>> eldest) {
            return size() > MRU_SIZE;
          }
        });
  }

  /**
   * Reports stochastic load balancer costs to JMX
   */
  public void updateStochasticCost(String tableName, String costFunctionName,
      String costFunctionDesc, Double value) {
    if (tableName == null || costFunctionName == null || value == null) {
      return;
    }

    Map<String, List<Object>> costs= stochasticCosts.get(tableName);
    if (costs== null) {
      costs= new ConcurrentHashMap<String, List<Object>>();
    }
    
    List<Object> descAndValue = new LinkedList<Object>();
    descAndValue.add(costFunctionDesc);
    descAndValue.add(value);

    costs.put(costFunctionName, descAndValue);
    stochasticCosts.put(tableName, costs);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder metricsRecordBuilder = metricsCollector.addRecord(metricsName);

    if (stochasticCosts != null) {
      for (String tableName : stochasticCosts.keySet()) {
        Map<String, List<Object>> costs = stochasticCosts.get(tableName);
        for (String key : costs.keySet()) {
          List<Object> descAndValue = costs.get(key);
          try {
            String desc = (String)descAndValue.get(0);
            Double value = (Double)descAndValue.get(1);
            String attrName = tableName + TABLE_FUNCTION_SEP + key;
            metricsRecordBuilder.addGauge(Interns.info(attrName, desc), value);
          } catch (Exception e) {
            LOG.warn("Add metrics for stochastic cost " + key + " failed:" + e.getMessage());
          }
        }
      }
    }
    metricsRegistry.snapshot(metricsRecordBuilder, all);
  }
}
