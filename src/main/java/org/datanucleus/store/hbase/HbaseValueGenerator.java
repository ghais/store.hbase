/**
 * Property of Digi-Net Technologies, Inc. 
 * 4420 Northwest 36th Avenue
 * Gainesville, FL 32606 USA 
 * All rights reserved.
 */
package org.datanucleus.store.hbase;

import java.util.Properties;
import java.util.UUID;

import org.datanucleus.store.valuegenerator.AbstractUUIDGenerator;

public class HbaseValueGenerator extends AbstractUUIDGenerator
{
    public HbaseValueGenerator(String name, Properties props)
    {
        super(name, props);
    }


    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.valuegenerator.AbstractUIDGenerator#getIdentifier()
     */
    @Override
    protected String getIdentifier()
    {
        return UUID.randomUUID().toString();
    }

}