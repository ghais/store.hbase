/**
 * Property of Digi-Net Technologies, Inc. 
 * 4420 Northwest 36th Avenue
 * Gainesville, FL 32606 USA 
 * All rights reserved.
 */
package org.datanucleus.test.models;

import java.util.List;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Ghais Issa <ghais.issa@convertglobal.com>
 */
@PersistenceCapable(detachable = "true")
public class JDOHasSerializable
{

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.UUIDHEX)
    private String key;

    @Persistent(defaultFetchGroup = "true", serialized = "true")
    private List<KitchenSink> serializable;

    /**
     * @return the key
     */
    public String getKey()
    {
        return key;
    }

    /**
     * @return the serializable
     */
    public List<KitchenSink> getSerializable()
    {
        return serializable;
    }

    /**
     * @param key the key to set
     */
    public void setKey(String key)
    {
        this.key = key;
    }

    /**
     * @param serializable the serializable to set
     */
    public void setSerializable(List<KitchenSink> serializable)
    {
        this.serializable = serializable;
    }

}
