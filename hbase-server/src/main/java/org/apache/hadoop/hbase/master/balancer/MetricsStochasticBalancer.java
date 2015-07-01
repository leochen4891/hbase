package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;

public class MetricsStochasticBalancer extends MetricsBalancer {
  /**
   * Use the stochastic source instead of the default source.
   */
  protected MetricsStochasticBalancerSource stochasticSource = null;

  public MetricsStochasticBalancer() {
    initSource();
  }

  /**
   * This function overrides the initSource in the MetricsBalancer, use
   * MetricsStochasticBalancerSource instead of the default MetricsBalancerSource.
   */
  @Override
  protected void initSource() {
    stochasticSource =
        CompatibilitySingletonFactory.getInstance(MetricsStochasticBalancerSource.class);
  }

  @Override
  public void balanceCluster(long time) {
    stochasticSource.updateBalanceCluster(time);
  }

  @Override
  public void incrMiscInvocations() {
    stochasticSource.incrMiscInvocations();
  }

  public void updateStochasticCost(String tableName, String costFunctionName,
      String costFunctionDesc, Double value) {
    stochasticSource.updateStochasticCost(tableName, costFunctionName, costFunctionDesc, value);
  }
}
