/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2016, Red Hat, Inc., and individual contributors
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

import javax.naming.Context;
import javax.naming.NamingException;

import org.kohsuke.MetaInfServices;
import org.wildfly.naming.client.NamingContextFactory;
import org.wildfly.naming.client.NamingProvider;
import org.wildfly.naming.client.remote.RemoteNamingProvider;
import org.wildfly.naming.client.util.FastHashtable;

/**
 * The naming context factory for EJB JNDI names.  This covers any names that begin with {@code ejb:}.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
@MetaInfServices
public final class EJBNamingContextFactory implements NamingContextFactory {
    public boolean supportsUriScheme(final NamingProvider namingProvider, final String nameScheme) {
        return namingProvider instanceof RemoteNamingProvider && "ejb".equals(nameScheme);
    }

    public Context createRootContext(final NamingProvider namingProvider, final String nameScheme, final FastHashtable<String, Object> env) throws NamingException {
        assert nameScheme.equals("ejb");
        return new EJBRootContext(namingProvider, env);
    }
}
