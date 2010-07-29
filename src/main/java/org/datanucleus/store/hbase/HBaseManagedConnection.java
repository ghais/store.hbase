/**********************************************************************
Copyright (c) 2009 Tatsuya Kawano and others. All rights reserved.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAResource;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;

/**
 * Implementation of a ManagedConnection.
 */
public class HBaseManagedConnection extends AbstractManagedConnection
{
	private HBaseConfiguration config;
	
	private Map<String, HTable> tables;
	
	private int referenceCount = 0;
	
	private int idleTimeoutMills = 30 * 1000; // 30 secs
	
	private long expirationTime;  
	
	private boolean isDisposed = false;
	
    public HBaseManagedConnection(HBaseConfiguration config)
    {
    	this.config = config; 
    	this.tables = new HashMap<String, HTable>();
    	disableExpirationTime();
    }
    
    public Object getConnection() 
    {
    	throw new NucleusDataStoreException("Unsopported Exception #getConnection() for " 
    			+ this.getClass().getName());
    }
    
    public HTable getHTable(String tableName) 
    {
    	HTable table = tables.get(tableName);
    	
        if (table == null)
        {
        	try 
        	{
				table = new HTable(config, tableName);
				tables.put(tableName, table);
			} 
        	catch (IOException e) 
        	{
	            throw new NucleusDataStoreException(e.getMessage(),e);
			}
        }
        
        return table;
    }

    public XAResource getXAResource()
    {
        return null;
    }
    
    public void close()
    {
        if (tables.size() == 0)
        {
            return;
        }
        for (int i=0; i<listeners.size(); i++)
        {
            ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPreClose();
        }
        try
        {
            //close something?
        }
        finally
        {
			for (int i=0; i<listeners.size(); i++)
			{
			    ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPostClose();
			}
        }
    }
    
    void incrementReferenceCount()
    {
        ++referenceCount;
        disableExpirationTime();
    }

    public void release()
    {
    	--referenceCount;

        if (referenceCount == 0)
        {
            close();
        	enableExpirationTime();
        } 
        else if (referenceCount < 0) 
    	{
    		throw new NucleusDataStoreException("Too many calls on release(): " + this);
    	}
    }

    private void enableExpirationTime() 
    {
    	this.expirationTime = System.currentTimeMillis() + idleTimeoutMills;
    }
    
    private void disableExpirationTime() 
    {
    	this.expirationTime = -1;
    }
    
    public void setIdleTimeoutMills(int mills) 
    {
    	this.idleTimeoutMills = mills;
    }
    
    public boolean isExpired() 
    {
    	return expirationTime > 0  &&  expirationTime > System.currentTimeMillis();
    }
    
    public void dispose() 
    {
    	isDisposed = true;
    	closeTables(tables);
    }

    public boolean isDisposed() 
    {
    	return isDisposed;
    }

    private void closeTables(Map<String, HTable> tables) 
    {
		for (String tableName : tables.keySet()) 
		{
			try 
			{
				HTable table = tables.get(tableName);
				table.close();
			} 
			catch (IOException e) 
			{
				// TODO: There can be multiple exceptions, so wrap them in together
			    throw new NucleusDataStoreException(e.getMessage(),e);
			}
		}
	}

}
