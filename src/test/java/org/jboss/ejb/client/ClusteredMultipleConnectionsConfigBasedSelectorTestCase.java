/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.ejb.client;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.jboss.ejb.client.remoting.ConfigBasedEJBClientContextSelector;
import org.jboss.ejb.client.test.common.DummyServer;
import org.jboss.ejb.client.test.common.EchoBean;
import org.jboss.ejb.client.test.common.EchoRemote;
import org.jboss.logging.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;

/**
 * Note that this test case *must* be run in a new JVM instance so that the {@link ConfigBasedEJBClientContextSelector}
 * is initialized with the correct set of properties that are set in the {@link #beforeClass()} of this test case. We
 * use the forkMode=always of the Maven surefire plugin to ensure this behavior (see the pom.xml of this project).
 *
 * This test case ensures that the number of receivers are kept at a minimum and that the max-connected-nodes for the cluster configuration is honored.
 *
 * @author Kristoffer Lundberg
 */
@SuppressWarnings("deprecation")
public class ClusteredMultipleConnectionsConfigBasedSelectorTestCase {
    private static final Logger logger = Logger.getLogger(ClusteredMultipleConnectionsConfigBasedSelectorTestCase.class);

    private static final String DUMMY_APP = "dummy-app";
    private static final String DUMMY_MODULE = "dummy-module";
    private static final String DUMMY_DISTINCT_NAME = "";
    private static final String ECHO_BEAN_NAME = EchoBean.class.getSimpleName();
    
    private static final int SERVER_COUNT = 4; 
    
    private static File configFile;
    private static List<DummyServer> servers;

    private static List<DummyServer> setupConfigurationAndServers(final File file) throws Exception
    {
      final List<DummyServer> serverList = new LinkedList<DummyServer>();
      
      final PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), "ISO-8859-1"));
      try {
        
        writer.println("remote.connectionprovider.create.options.org.xnio.Options.SSL_ENABLED=false");
        
        final StringBuilder serverNames = new StringBuilder(512);
        
        for (int i = 0, port = 7000; i < SERVER_COUNT; i++, port++) {
          final String serverName = "localhost_" + port;
          
          serverNames.append(serverName + ",");
          
          // Write server config
          writer.print("remote.connection.");
          writer.print(serverName);
          writer.println(".host=localhost");
  
          writer.print("remote.connection.");
          writer.print(serverName);
          writer.print(".port=");
          writer.println(port);
          
          writer.print("remote.connection.");
          writer.print(serverName);
          writer.println(".connect.options.org.xnio.Options.SASL_POLICY_NOANONYMOUS=false");
          
          final DummyServer server = new DummyServer("localhost", port, serverName);
          server.start();
          server.register(DUMMY_APP, DUMMY_MODULE, DUMMY_DISTINCT_NAME, ECHO_BEAN_NAME, new EchoBean());
          serverList.add(server);
        }
        
        // Remove trailing ,
        serverNames.setLength(serverNames.length() - 1);
  
        // List all available servers
        writer.println("remote.connections=" + serverNames.toString());
        
        // Write cluster config
        writer.print("remote.clusters=");
        writer.println(DummyServer.CLUSTER_NAME);
  
        // Write connect options
        writer.print("remote.cluster.");
        writer.print(DummyServer.CLUSTER_NAME);
        writer.println(".connect.options.org.xnio.Options.SASL_POLICY_NOANONYMOUS=false");
        
        // Write config to only allow one node
        writer.print("remote.cluster.");
        writer.print(DummyServer.CLUSTER_NAME);
        writer.println(".max-allowed-connected-nodes=1");
      } finally {
        writer.close();
      }
      
      return serverList;
    }
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        try {
          configFile = File.createTempFile("jboss-ejb-client-cluster-config", "properties");
          servers = setupConfigurationAndServers(configFile);
          
          System.setProperty("jboss.ejb.client.properties.file.path", configFile.toURI().toURL().getPath());
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        }
    }


    @AfterClass
    public static void afterClass() throws Exception {
        if (servers != null) {
          for (final DummyServer server : servers) {
            server.stop();
          }
        }
        
        if (configFile != null) {
            configFile.delete();
        }
    }

    @Test
    public void testOnlyOneConnection() throws Exception {
        final Properties properties = EJBClientPropertiesLoader.loadEJBClientProperties();
        final EJBClientConfiguration ejbClientConfiguration = new PropertiesBasedEJBClientConfiguration(properties);
        final ConfigBasedEJBClientContextSelector configBasedEJBClientContextSelector = new ConfigBasedEJBClientContextSelector(ejbClientConfiguration);

        EJBClientContext.setSelector(configBasedEJBClientContextSelector);
        
        final CountDownLatch latch = new CountDownLatch(servers.size());
        
        EJBClientContext.requireCurrent().getOrCreateClusterContext(DummyServer.CLUSTER_NAME).registerListener(new ClusterContext.ClusterContextListener()
        {
          @Override
          public void clusterNodesAdded(String clusterName, ClusterNodeManager... nodes)
          {
            if (logger.isInfoEnabled()) {
              final StringBuilder sb = new StringBuilder();
              for (final ClusterNodeManager node : nodes) {
                sb.append(node.getNodeName()).append(", ");
              }
  
              if (nodes.length > 0) {
                sb.setLength(sb.length() - 2);
              }
  
              logger.infof("Got nodes: [%s]", sb.toString());
            }
            latch.countDown();
          }
        });

        final StatelessEJBLocator<EchoRemote> locator = new StatelessEJBLocator<EchoRemote>(EchoRemote.class, DUMMY_APP, DUMMY_MODULE, ECHO_BEAN_NAME, DUMMY_DISTINCT_NAME);
        final EchoRemote echo = EJBClient.createProxy(locator);
        echo.echo(DUMMY_APP);

        for (final DummyServer server: servers) {
          server.sendClusterTopologyMessageToClients();
        }
        
        latch.await();

        Assert.assertEquals(1, EJBClientContext.requireCurrent().getClusterContext(DummyServer.CLUSTER_NAME).getConnectedAndDeployedNodes(locator).size());
    }
}
