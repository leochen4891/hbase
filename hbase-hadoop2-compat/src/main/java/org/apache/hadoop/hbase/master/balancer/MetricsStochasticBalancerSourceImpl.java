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
   * Save the costs to feed to JMX.
   */
  public void updateStochasticCost(String tableName, String costFunctionName,
      String costFunctionDesc, Double value) {
    if (null == tableName || null == costFunctionName) {
      return;
    }

    Map<String, Double> map = stochasticCosts.get(tableName);
    if (null == map) {
      map = new ConcurrentHashMap<String, Double>();
    }

    map.put(costFunctionName, value);
    stochasticCosts.put(tableName, map);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder metricsRecordBuilder = metricsCollector.addRecord(metricsName);

    if (null != stochasticCosts) {
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
