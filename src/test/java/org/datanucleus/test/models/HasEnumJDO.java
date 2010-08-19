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
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasEnumJDO
{
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.UUIDHEX)
    private String key;

    @Persistent
    private MyEnum myEnum;

    @Persistent
    private MyEnum[] myEnumArray;

    @Persistent
    private List<MyEnum> myEnumList;

    public String getKey()
    {
        return key;
    }

    public void setKey(String key)
    {
        this.key = key;
    }

    public MyEnum getMyEnum()
    {
        return myEnum;
    }

    public void setMyEnum(MyEnum myEnum)
    {
        this.myEnum = myEnum;
    }

    public MyEnum[] getMyEnumArray()
    {
        return myEnumArray;
    }

    public void setMyEnumArray(MyEnum[] myEnumArray)
    {
        this.myEnumArray = myEnumArray;
    }

    public List<MyEnum> getMyEnumList()
    {
        return myEnumList;
    }

    public void setMyEnumList(List<MyEnum> myEnumList)
    {
        this.myEnumList = myEnumList;
    }

    public enum MyEnum {
        V1, V2
    };
}
