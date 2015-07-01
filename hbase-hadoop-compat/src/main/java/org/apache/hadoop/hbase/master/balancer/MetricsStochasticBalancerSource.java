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

/**
 * This interface extends the basic metrics balancer source to add a function 
 * to report metrics that related to stochastic load balancer. The purpose is to 
 * offer an insight to the internal cost calculations that can be useful to tune
 * the balancer. For details, refer to HBASE-13965
 */
public interface MetricsStochasticBalancerSource extends MetricsBalancerSource {

  /**
   * The function that report stochastic load balancer costs to JMX
   */
  public void updateStochasticCost(String tableName, String costFunctionName,
      String costFunctionDesc, Double value);
}
