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

package org.apache.hadoop.hbase;


import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.balancer.BalancerTestBase;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;



@Category({ MiscTests.class, MediumTests.class })
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestStochasticBalancerJmxMetrics extends BalancerTestBase {
  private static final Log LOG = LogFactory.getLog(TestStochasticBalancerJmxMetrics.class);
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static int connectorPort = 61120;
  private static StochasticLoadBalancer loadBalancer;
  /**
   * a simple cluster for testing JMX.
   */
  private static int[] mockCluster = new int[] { 0, 1, 2, 3 };

  /**
   * Setup the environment for the test.
   */
  @BeforeClass
  public static void setupBeforeClass() throws Exception {

    Configuration conf = UTIL.getConfiguration();

    conf.setClass("hbase.util.ip.to.rack.determiner", MockMapping.class, DNSToSwitchMapping.class);
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 0.75f);
    conf.setFloat("hbase.regions.slop", 0.0f);
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, JMXListener.class.getName());
    conf.setInt("regionserver.rmi.registry.port", connectorPort);

    loadBalancer = new StochasticLoadBalancer();
    loadBalancer.setConf(conf);

    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * To test if the attributes of stochastic load balancer are added to JMX.
   */
  @Test
  public void testJmxAttributes() throws Exception {
    Map<ServerName, List<HRegionInfo>> clusterState = mockClusterServers(mockCluster);
    loadBalancer.balanceCluster(clusterState);

    String tableName = StochasticLoadBalancer.getTableName(clusterState);

    JMXConnector connector =
        JMXConnectorFactory.connect(JMXListener.buildJMXServiceURL(connectorPort, connectorPort));
    MBeanServerConnection mb = connector.getMBeanServerConnection();

    // confirm that all the attributes are in the attribute list
    // create the object name
    Hashtable<String, String> pairs = new Hashtable<>();
    pairs.put("service", "HBase");
    pairs.put("name", "Master");
    pairs.put("sub", "Balancer");
    ObjectName target = new ObjectName("Hadoop", pairs);
    MBeanInfo beanInfo = mb.getMBeanInfo(target);
    
    // put all the attributes in a hashset for quick search
    Set<String> existingAttrs= new HashSet<String>();
    for (MBeanAttributeInfo attrInfo : beanInfo.getAttributes()) {
      existingAttrs.add(attrInfo.getName());
    }
    // printAttributesToLog(mb, target);

    // confirm that all the required attributes are in the set
    String[] functionNames = loadBalancer.getCostFunctionNames();
    for (String functionName : functionNames) {
      String attrName = StochasticLoadBalancer.composeAttributeName(tableName, functionName);
      assertTrue("Attribute " + attrName + " can not be found in " + target, 
          existingAttrs.contains(attrName));
    }

    connector.close();
  }

  /**
   * Print all the domains in the JMX.
   */
  private static void printDomainsToLog(MBeanServerConnection mb) {
    try {
      String[] domains = mb.getDomains();
      Arrays.sort(domains);
      for (int i = 0; i < domains.length; i++) {
        LOG.info("++++ domain[" + i + "] = " + domains[i]);
      }
      LOG.info("++++ default domain = " + mb.getDefaultDomain());

      LOG.info("++++");
      LOG.info("++++ MBean count = " + mb.getMBeanCount());
      int index = 0;
      Set<ObjectName> names = new HashSet<ObjectName>(mb.queryNames(null, null));
      for (ObjectName name : names) {
        LOG.info("++++   " + index + ":" + name);
        index++;
      }
    } catch (Exception e) {
      LOG.info("++++ printDomainsToLog got exception: " + e.getMessage());
    }
  }

  /** 
   * Print all the attributes in the MBean.
   */
  private static void printAttributesToLog(MBeanServerConnection mb, ObjectName target) {
    try {
      MBeanInfo beanInfo = mb.getMBeanInfo(target);
      LOG.info("++++ Attribute count = " + beanInfo.getAttributes().length);
      int index = 0;
      for (MBeanAttributeInfo attrInfo : beanInfo.getAttributes()) {
        LOG.info("++++   " + index + ":" + attrInfo.getName() + ":"
            + mb.getAttribute(target, attrInfo.getName()));
        index++;
      }
    } catch (Exception e) {
      LOG.info("++++ printAttributesToLog got exception: " + e.getMessage());
    }
  }
}
