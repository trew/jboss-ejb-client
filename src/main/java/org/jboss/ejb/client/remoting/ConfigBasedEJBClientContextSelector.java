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

package org.jboss.ejb.client.remoting;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.ejb.client.ClusterContext;
import org.jboss.ejb.client.ClusterNodeManager;
import org.jboss.ejb.client.EJBClientConfiguration;
import org.jboss.ejb.client.EJBClientContext;
import org.jboss.ejb.client.EJBClientContextIdentifier;
import org.jboss.ejb.client.EJBClientContextListener;
import org.jboss.ejb.client.EJBReceiver;
import org.jboss.ejb.client.EJBReceiverContext;
import org.jboss.ejb.client.IdentityEJBClientContextSelector;
import org.jboss.ejb.client.Logs;
import org.jboss.logging.Logger;
import org.jboss.remoting3.Connection;
import org.jboss.remoting3.Endpoint;
import org.xnio.OptionMap;

/**
 * An EJB client context selector which uses {@link EJBClientConfiguration} to create {@link org.jboss.ejb.client.remoting.RemotingConnectionEJBReceiver}s.
 *
 * @author Jaikiran Pai
 */
public class ConfigBasedEJBClientContextSelector implements IdentityEJBClientContextSelector {

    private static final Logger logger = Logger.getLogger(ConfigBasedEJBClientContextSelector.class);

    protected final EJBClientConfiguration ejbClientConfiguration;
    protected final EJBClientContext ejbClientContext;
    private final RemotingEndpointManager remotingEndpointManager = new RemotingEndpointManager();
    private final RemotingConnectionManager remotingConnectionManager = new RemotingConnectionManager();

    private final ConcurrentMap<EJBClientContextIdentifier, EJBClientContext> identifiableContexts = new ConcurrentHashMap<EJBClientContextIdentifier, EJBClientContext>();
    private final Initializer initializer;

    private volatile boolean receiversSetup;

    private static final boolean reconnectOnAuthenticationFailures = Boolean.valueOf(
            System.getProperty("jboss-ejb-client.reconnectOnAuthenticationFailures", "true")).booleanValue();


    /**
     * Creates a {@link ConfigBasedEJBClientContextSelector} using the passed <code>ejbClientConfiguration</code>.
     * <p/>
     * This constructor creates a {@link EJBClientContext} and uses the passed <code>ejbClientConfiguration</code> to create and
     * associated EJB receivers to that context. If the passed <code>ejbClientConfiguration</code> is null, then this selector will create a {@link EJBClientContext}
     * without any associated EJB receivers.
     *
     * @param ejbClientConfiguration The EJB client configuration to use
     */
    public ConfigBasedEJBClientContextSelector(final EJBClientConfiguration ejbClientConfiguration) {
        this(ejbClientConfiguration, null);
    }

    /**
     * Creates a {@link ConfigBasedEJBClientContextSelector} using the passed <code>ejbClientConfiguration</code>.
     * <p/>
     * This constructor creates a {@link EJBClientContext} and uses the passed <code>ejbClientConfiguration</code> to create and
     * associated EJB receivers to that context. If the passed <code>ejbClientConfiguration</code> is null, then this selector will create a {@link EJBClientContext}
     * without any associated EJB receivers.
     *
     * @param ejbClientConfiguration The EJB client configuration to use
     * @param classLoader            The classloader that will be used to {@link EJBClientContext#create(org.jboss.ejb.client.EJBClientConfiguration, ClassLoader) create the EJBClientContext}
     */
    public ConfigBasedEJBClientContextSelector(final EJBClientConfiguration ejbClientConfiguration, final ClassLoader classLoader) {
      this(ejbClientConfiguration, classLoader, null);
    }

    /**
     * Creates a {@link ConfigBasedEJBClientContextSelector} using the passed <code>ejbClientConfiguration</code>.
     * <p/>
     * This constructor creates a {@link EJBClientContext} and uses the passed <code>ejbClientConfiguration</code> to create and
     * associated EJB receivers to that context. If the passed <code>ejbClientConfiguration</code> is null, then this selector will create a {@link EJBClientContext}
     * without any associated EJB receivers.
     *
     * @param ejbClientConfiguration The EJB client configuration to use
     * @param classLoader The classloader that will be used to {@link EJBClientContext#create(org.jboss.ejb.client.EJBClientConfiguration, ClassLoader) create the EJBClientContext}
     * @param initializer The Initializer to use or null for default.
     */
    public ConfigBasedEJBClientContextSelector(final EJBClientConfiguration ejbClientConfiguration, final ClassLoader classLoader, final Initializer initializer) {
        this.ejbClientConfiguration = ejbClientConfiguration;
        // create a empty context
        if (classLoader == null) {
            this.ejbClientContext = EJBClientContext.create(this.ejbClientConfiguration);
        } else {
            this.ejbClientContext = EJBClientContext.create(this.ejbClientConfiguration, classLoader);
        }
        // register a EJB client context listener which we will use to close endpoints/connections that we created,
        // when the EJB client context closes
        this.ejbClientContext.registerEJBClientContextListener(new ContextCloseListener());

        this.initializer = initializer;
    }

    @Override
    public EJBClientContext getCurrent() {
        if (this.receiversSetup) {
            return this.ejbClientContext;
        }
        synchronized (this) {
            if (this.receiversSetup) {
                return this.ejbClientContext;
            }
            try {
                // now setup the receivers (if any) for the context
                if (this.ejbClientConfiguration == null) {
                    logger.debugf("EJB client context %s will have no EJB receivers associated with it since there was no " +
                            "EJB client configuration available to create the receivers", this.ejbClientContext);
                    return this.ejbClientContext;
                }
                try {
                    (initializer != null ? initializer : new DefaultInitializer()).initialize(this, this.ejbClientContext);
                } catch (IOException ioe) {
                    logger.warn("EJB client context " + this.ejbClientContext + " will have no EJB receivers due to an error setting up EJB receivers", ioe);
                }
            } finally {
                this.receiversSetup = true;
            }

        }
        return this.ejbClientContext;
    }

    private void registerReconnectHandler(final ReconnectHandler reconnectHandler, String host, int port) {
        // add a reconnect handler for this connection
        if (reconnectHandler != null) {
            this.ejbClientContext.registerReconnectHandler(reconnectHandler);
            logger.debug("Registered a reconnect handler in EJB client context " + this.ejbClientContext + " for remote://" + host + ":" + port);
        }
    }

    @Override
    public void registerContext(final EJBClientContextIdentifier identifier, final EJBClientContext context) {
        final EJBClientContext previousRegisteredContext = this.identifiableContexts.putIfAbsent(identifier, context);
        if (previousRegisteredContext != null) {
            throw Logs.MAIN.ejbClientContextAlreadyRegisteredForIdentifier(identifier);
        }
    }

    @Override
    public EJBClientContext unRegisterContext(final EJBClientContextIdentifier identifier) {
        return this.identifiableContexts.remove(identifier);
    }

    @Override
    public EJBClientContext getContext(final EJBClientContextIdentifier identifier) {
        return this.identifiableContexts.get(identifier);
    }

    private class ContextCloseListener implements EJBClientContextListener {

        @Override
        public void contextClosed(EJBClientContext ejbClientContext) {
            // close the endpoint and connection manager we had used to create the endpoints and connections
            // for the EJB client context that just closed
            remotingConnectionManager.safeClose();
            remotingEndpointManager.safeClose();
        }

        @Override
        public void receiverRegistered(EJBReceiverContext receiverContext) {
        }

        @Override
        public void receiverUnRegistered(EJBReceiverContext receiverContext) {
        }
    }

    /**
     * Initializes the {@link ConfigBasedEJBClientContextSelector}, e.g. setting up {@link EJBReceiver}s.
     *
     * @author kristoffer@cambio.se
     */
    public abstract static class Initializer {
        private static final InetAddress localHost;

        static {
            try {
                localHost = InetAddress.getLocalHost();
            } catch (IOException ioe) {
                throw new IllegalStateException("Unable to fetch localhost", ioe);
            }
        }

        /**
         * Creates an {@link Endpoint} based on the supplied paramaters, and tracks the created instance.
         */
        protected Endpoint getEndpoint(final ConfigBasedEJBClientContextSelector selector, final String endpointName, final OptionMap endpointCreationOptions, final OptionMap remotingConnectionProviderOptions) throws IOException {
            return selector.remotingEndpointManager.getEndpoint(endpointName, endpointCreationOptions, remotingConnectionProviderOptions);
        }
        
        /**
         * Creates a {@link Connection} using the supplied parameters and tracks the {@link Connection}.
         */
        protected Connection getConnection(final ConfigBasedEJBClientContextSelector selector, final Endpoint endpoint, final String host, final int port, EJBClientConfiguration.CommonConnectionCreationConfiguration connectionConfiguration) throws IOException {
            return selector.remotingConnectionManager.getConnection(endpoint, host, port, connectionConfiguration);
        }
        
        /**
         * Creates a {@link ClusterNodeManager} using the supplied parameters, with a {@link ClusterNode} with a single {@link ClientMapping} using localhost as sourceNetworkAddress and zero as sourceNetworkMaskBits.
         */
        protected ClusterNodeManager createClusterNodeManager(final ConfigBasedEJBClientContextSelector selector, final ClusterContext clusterContext, final String clusterName, final String nodeName, final String destinationAddress, final int destinationPort, final Endpoint endpoint, final EJBClientConfiguration ejbClientConfiguration) {
            return createClusterNodeManager(selector, clusterContext, clusterName, nodeName, new ClientMapping[] { new ClientMapping(localHost, 0, destinationAddress, destinationPort) }, endpoint, ejbClientConfiguration);
        }
        
        /**
         * Creates a {@link ClusterNodeManager} using the supplied parameters.
         */
        protected ClusterNodeManager createClusterNodeManager(final ConfigBasedEJBClientContextSelector selector, final ClusterContext clusterContext, final String clusterName, final String nodeName, final ClientMapping[] clientMappings, final Endpoint endpoint, final EJBClientConfiguration ejbClientConfiguration) {
            return new RemotingConnectionClusterNodeManager(clusterContext, new ClusterNode(clusterName, nodeName, clientMappings), endpoint, ejbClientConfiguration);
        }
        
        /**
         * Initializes the {@link ConfigBasedEJBClientContextSelector}, e.g. creating {@link EJBReceiver}s.
         */
        public abstract void initialize(final ConfigBasedEJBClientContextSelector selector, final EJBClientContext ejbClientContext) throws IOException;
    }

    /**
     * The default {@link Initializer}, creating initial connections to connect to the cluster, before receiving a cluster view.
     *
     * @author kristoffer@cambio.se
     */
    protected static class DefaultInitializer extends Initializer {
        public DefaultInitializer() {
        }

        @Override
        public void initialize(final ConfigBasedEJBClientContextSelector selector, final EJBClientContext ejbClientContext) throws IOException {
            if (!selector.ejbClientConfiguration.getConnectionConfigurations().hasNext()) {
                // no connections configured so no EJB receivers to create
                return;
            }
            // create the endpoint
            final Endpoint endpoint = selector.remotingEndpointManager.getEndpoint(selector.ejbClientConfiguration.getEndpointName(), selector.ejbClientConfiguration.getEndpointCreationOptions(), selector.ejbClientConfiguration.getRemoteConnectionProviderCreationOptions());
    
            final Iterator<EJBClientConfiguration.RemotingConnectionConfiguration> connectionConfigurations = selector.ejbClientConfiguration.getConnectionConfigurations();
            int successfulEJBReceiverRegistrations = 0;
            while (connectionConfigurations.hasNext()) {
                final EJBClientConfiguration.RemotingConnectionConfiguration connectionConfiguration = connectionConfigurations.next();
                final String host = connectionConfiguration.getHost();
                final int port = connectionConfiguration.getPort();
                final int MAX_RECONNECT_ATTEMPTS = 65535; // TODO: Let's keep this high for now and later allow configuration and a smaller default value
                // create a re-connect handler (which will be used on connection breaking down)
    
                final ReconnectHandler reconnectHandler = new EJBClientContextConnectionReconnectHandler(ejbClientContext, endpoint, host, port, connectionConfiguration, MAX_RECONNECT_ATTEMPTS);
                // if the connection attempt shouldn't be "eager" then we just register the reconnect handler to the client context so that the reconnect handler
                // can attempt the connection whenever it's required to do so.
                if (!connectionConfiguration.isConnectEagerly()) {
                    selector.registerReconnectHandler(reconnectHandler, host, port);
                    logger.debug("Connection to host: " + host + " and port: " + port + ", in EJB client context: " + selector.ejbClientContext + ", is configured to be attempted lazily. Skipping connection creation for now");
    
                } else {
                    try {
                        // wait for the connection to be established
                        final Connection connection = selector.remotingConnectionManager.getConnection(endpoint, host, port, connectionConfiguration);
                        // create a remoting EJB receiver for this connection
                        final EJBReceiver remotingEJBReceiver = new RemotingConnectionEJBReceiver(connection, reconnectHandler, connectionConfiguration.getChannelCreationOptions());
                        // associate it with the client context
                        selector.ejbClientContext.registerEJBReceiver(remotingEJBReceiver);
                        // keep track of successful registrations for logging purposes
                        successfulEJBReceiverRegistrations++;
                    } catch (javax.security.sasl.SaslException e) {
                        // just log the warn but don't throw an exception. Move onto the next connection configuration (if any)
                        logger.warn("Could not register a EJB receiver for connection to " + host + ":" + port, e);
    
                        if( reconnectOnAuthenticationFailures ) {
                            selector.registerReconnectHandler(reconnectHandler, host, port);
                        }
                        else {
                            logger.debug("Skipped registering a reconnect handler due to an authentication error!");
                        }
                    } catch (Exception e) {
                        // just log the warn but don't throw an exception. Move onto the next connection configuration (if any)
                        logger.warn("Could not register a EJB receiver for connection to " + host + ":" + port, e);
    
                        selector.registerReconnectHandler(reconnectHandler, host, port);
                    }
                }
            }
            logger.debug("Registered " + successfulEJBReceiverRegistrations + " remoting EJB receivers for EJB client context " + selector.ejbClientContext);
        }
    }
}
