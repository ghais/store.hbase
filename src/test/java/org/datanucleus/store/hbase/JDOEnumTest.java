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
package org.datanucleus.store.hbase;

import static org.datanucleus.test.models.HasEnumJDO.MyEnum.V1;
import static org.datanucleus.test.models.HasEnumJDO.MyEnum.V2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import javax.jdo.PersistenceManager;

import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.HasEnumJDO;
import org.datanucleus.test.models.HasEnumJDO.MyEnum;
import org.junit.Test;

/**
 */
public class JDOEnumTest extends BaseTest
{

    @Test
    public void testRoundtrip()
    {
        PersistenceManager pm = getPersistenceManager();
        HasEnumJDO pojo = new HasEnumJDO();
        pojo.setMyEnum(V1);
        pojo.setMyEnumArray(new MyEnum[]{V2, V1, V2});
        pojo.setMyEnumList(Utils.newArrayList(V1, V2, V1));
        beginTxn();
        pm.makePersistent(pojo);
        commitTxn();

        beginTxn();
        pojo = pm.getObjectById(HasEnumJDO.class, pojo.getKey());
        assertEquals(HasEnumJDO.MyEnum.V1, pojo.getMyEnum());
        assertTrue(Arrays.equals(new MyEnum[]{V2, V1, V2}, pojo.getMyEnumArray()));
        assertEquals(Utils.newArrayList(V1, V2, V1), pojo.getMyEnumList());
        commitTxn();
    }

    @Test
    public void testRoundtrip_Null()
    {
        PersistenceManager pm = getPersistenceManager();
        HasEnumJDO pojo = new HasEnumJDO();
        beginTxn();
        pm.makePersistent(pojo);
        commitTxn();

        beginTxn();
        pojo = pm.getObjectById(HasEnumJDO.class, pojo.getKey());
        assertNull(pojo.getMyEnum());
        assertNull(pojo.getMyEnumArray());
        assertNull(pojo.getMyEnumList());
        commitTxn();
    }

    @Test
    public void testRoundtrip_NullContainerVals()
    {
        PersistenceManager pm = getPersistenceManager();
        HasEnumJDO pojo = new HasEnumJDO();
        pojo.setMyEnumArray(new MyEnum[]{null, V2});
        pojo.setMyEnumList(Utils.newArrayList(null, V2));
        beginTxn();
        pm.makePersistent(pojo);
        commitTxn();

        beginTxn();
        pojo = pm.getObjectById(HasEnumJDO.class, pojo.getKey());
        assertNull(pojo.getMyEnum());
        assertTrue(Arrays.equals(new MyEnum[]{null, V2}, pojo.getMyEnumArray()));
        assertEquals(Utils.newArrayList(null, V2), pojo.getMyEnumList());
        commitTxn();
    }
}
