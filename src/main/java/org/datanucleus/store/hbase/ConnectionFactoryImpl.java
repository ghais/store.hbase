/**********************************************************************
Copyright (c) 2009 Erik Bengtson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
***********************************************************************/
package org.datanucleus.store.hbase;

import java.util.Map;

import org.datanucleus.OMFContext;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;

/**
 * Implementation of a ConnectionFactory for HBase.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    private HBaseConnectionPool connectionPool;

    /**
     * Constructor.
     * @param omfContext The OMF context
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(OMFContext omfContext, String resourceType)
    {
        super(omfContext, resourceType);
        HBaseStoreManager storeManager = (HBaseStoreManager) omfContext.getStoreManager();
        connectionPool = new HBaseConnectionPool();
        connectionPool.setTimeBetweenEvictionRunsMillis(storeManager.getPoolTimeBetweenEvictionRunsMillis());
    }

    /**
     * Obtain a connection from the Factory. The connection will be enlisted within the {@link org.datanucleus.Transaction} 
     * associated to the <code>poolKey</code> if "enlist" is set to true.
     * @param poolKey the pool that is bound the connection during its lifecycle (or null)
     * @param options Any options for then creating the connection
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(Object poolKey, Map transactionOptions)
    {
        HBaseStoreManager storeManager = (HBaseStoreManager) omfContext.getStoreManager();
               
        HBaseManagedConnection managedConnection = connectionPool.getPooledConnection();
        if (managedConnection == null) 
        {
            managedConnection = new HBaseManagedConnection(storeManager.getHbaseConfig());
            managedConnection.setIdleTimeoutMills(storeManager.getPoolMinEvictableIdleTimeMillis());
            connectionPool.registerConnection(managedConnection);
        }
        managedConnection.incrementReferenceCount();
        return managedConnection;
    }
}
