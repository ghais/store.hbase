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

import java.io.Serializable;
import java.util.Date;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * Based on Google App Engine Datanucleus plugin
 */
@PersistenceCapable(detachable = "true")
public class DetachableJDO implements Serializable
{

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.UUIDHEX)
    String id;

    @Persistent
    String val;

    @Persistent
    Date date;

    public String getId()
    {
        return id;
    }

    public String getVal()
    {
        return val;
    }

    public void setVal(String val)
    {
        this.val = val;
    }

    public Date getDate()
    {
        return date;
    }

    public void setDate(Date date)
    {
        this.date = date;
    }
}