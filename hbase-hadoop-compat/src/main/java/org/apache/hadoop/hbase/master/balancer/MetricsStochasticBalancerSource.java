package org.apache.hadoop.hbase.master.balancer;

public interface MetricsStochasticBalancerSource extends MetricsBalancerSource {
  public void updateStochasticCost(String tableName, String costFunctionName,
      String costFunctionDesc, Double value);
}
