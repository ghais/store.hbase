/**********************************************************************
 * Copyright (c) 2010 Ghais Issa and others. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Contributors :
 * ...
 ************************************************************************/
package org.datanucleus.test.models;

import java.util.List;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import org.datanucleus.store.hbase.Utils;

/**
 * @author Ghais Issa <ghais.issa@convertglobal.com>
 */
@PersistenceCapable
public class HasSingleCollection
{
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.UNSPECIFIED, customValueStrategy = "uuid")
    String key;

    @Persistent(defaultFetchGroup = "true")
    List<String> items;

    /**
     * @return the key
     */
    public String getKey()
    {
        return key;
    }

    /**
     * @return the items
     */
    public List<String> getItems()
    {
        return items;
    }

    /**
     * @param key the key to set
     */
    public void setKey(String key)
    {
        this.key = key;
    }

    /**
     * @param items the items to set
     */
    public void setItems(List<String> items)
    {
        this.items = items;
    }

    public static HasSingleCollection newHasSingleCollectionJDO()
    {
        HasSingleCollection pojo = new HasSingleCollection();
        pojo.setItems(Utils.newArrayList("s", "o", "r", "r", "y"));
        return pojo;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof HasSingleCollection))
        {
            return false;
        }
        HasSingleCollection other = (HasSingleCollection) obj;
        if (key == null)
        {
            if (other.key != null)
            {
                return false;
            }
        }
        else if (!key.equals(other.key))
        {
            return false;
        }
        return true;
    }

}
