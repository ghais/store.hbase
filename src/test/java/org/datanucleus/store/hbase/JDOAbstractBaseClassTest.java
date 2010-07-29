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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.identity.StringIdentity;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.test.BaseTest;
import org.datanucleus.test.models.AbstractBaseClassesJDO.Concrete1;
import org.datanucleus.test.models.AbstractBaseClassesJDO.Concrete2;
import org.datanucleus.test.models.AbstractBaseClassesJDO.Concrete3;
import org.datanucleus.test.models.AbstractBaseClassesJDO.Concrete4;
import org.junit.Test;

/**
 */
public class JDOAbstractBaseClassTest extends BaseTest
{
    @SuppressWarnings("unchecked")
    @Test
    public void testConcrete() throws IOException, ClassNotFoundException
    {
        PersistenceManager pm = null;
        Concrete1 concrete = new Concrete1();
        concrete.setBase1Str("base 1");
        concrete.setConcrete1Str("concrete");
        Concrete3 concrete3 = new Concrete3();
        concrete3.setStr("str3");
        concrete.setConcrete3(concrete3);
        Concrete4 concrete4a = new Concrete4();
        concrete4a.setStr("str4a");
        Concrete4 concrete4b = new Concrete4();
        concrete4b.setStr("str4b");
        concrete.setConcrete4(Utils.newArrayList(concrete4a, concrete4b));

        pm = PMF.get().getPersistenceManager();
        try
        {
            pm.makePersistent(concrete);
        }
        finally
        {
            pm.close();
        }
        Result concreteEntity = get("Concrete1", Bytes.toBytes(concrete.getId()));
        Result concrete3Entity = get("Concrete3", Bytes.toBytes(concrete3.getId()));
        Result concrete4aEntity = get("Concrete4", Bytes.toBytes(concrete4a.getId()));
        Result concrete4bEntity = get("Concrete4", Bytes.toBytes(concrete4b.getId()));

        // Check for 5 attributes including key
        assertEquals(5, concreteEntity.size());
        // Check attributes
        assertEquals("base 1", getStrValue(concreteEntity, "Concrete1", "base1Str"));
        StringIdentity concrete3Id = deserialize(StringIdentity.class, getBytes(concreteEntity, "Concrete1", "concrete3"));
        assertEquals(concrete3Id.getKey(), concrete3.getId());
        List<StringIdentity> concrete4Ids = deserialize(ArrayList.class, getBytes(concreteEntity, "Concrete1", "concrete4"));
        assertEquals(Utils.newArrayList(new StringIdentity(concrete4a.getClass(), concrete4a.getId()),
            new StringIdentity(concrete4b.getClass(), concrete4b.getId())), concrete4Ids);

        // Check for 2 attributes including key
        assertEquals(2, concrete3Entity.size());
        // Check attributes
        assertEquals("str3", getStrValue(concrete3Entity, "Concrete3", "str"));

        // Check for 2 attributes including key
        assertEquals(2, concrete4aEntity.size());
        // Check attributes
        assertEquals("str4a", getStrValue(concrete4aEntity, "Concrete4", "str"));

        // Check for 2 attributes including key
        assertEquals(2, concrete4bEntity.size());
        // Check attributes
        assertEquals("str4b", getStrValue(concrete4bEntity, "Concrete4", "str"));

        pm = PMF.get().getPersistenceManager();
        try
        {
            concrete = pm.getObjectById(concrete.getClass(), concrete.getId());
            assertEquals("base 1", concrete.getBase1Str());
            assertEquals("concrete", concrete.getConcrete1Str());
            assertEquals(concrete3.getId(), concrete.getConcrete3().getId());
            assertEquals(concrete3.getStr(), concrete.getConcrete3().getStr());
            assertEquals(2, concrete.getConcrete4().size());
            assertEquals(concrete4a.getId(), concrete.getConcrete4().get(0).getId());
            assertEquals(concrete4a.getStr(), concrete.getConcrete4().get(0).getStr());
            assertEquals(concrete4b.getId(), concrete.getConcrete4().get(1).getId());
            assertEquals(concrete4b.getStr(), concrete.getConcrete4().get(1).getStr());

            concrete.setBase1Str("not base 1");
            concrete.setConcrete1Str("not concrete");

            concrete.getConcrete3().setStr("blam3");
            concrete.getConcrete4().get(0).setStr("blam4");
            concrete.getConcrete4().remove(1);
        }
        finally
        {
            pm.close();
        }

        concreteEntity = get("Concrete1", Bytes.toBytes(concrete.getId()));
        concrete3Entity = get("Concrete3", Bytes.toBytes(concrete3.getId()));
        concrete4aEntity = get("Concrete4", Bytes.toBytes(concrete4a.getId()));

        // Check for 5 attributes including key
        assertEquals(5, concreteEntity.size());
        // Check attributes
        assertEquals("not base 1", getStrValue(concreteEntity, "Concrete1", "base1Str"));
        assertEquals("not concrete", getStrValue(concreteEntity, "Concrete1", "concrete1Str"));
        concrete3Id = deserialize(StringIdentity.class, getBytes(concreteEntity, "Concrete1", "concrete3"));
        assertEquals(concrete3Id.getKey(), concrete3.getId());
        concrete4Ids = deserialize(ArrayList.class, getBytes(concreteEntity, "Concrete1", "concrete4"));
        assertEquals(Utils.newArrayList(new StringIdentity(concrete4a.getClass(), concrete4a.getId())), concrete4Ids);

        // Check for 2 attributes including key
        assertEquals(2, concrete3Entity.size());
        // Check attributes
        assertEquals("blam3", getStrValue(concrete3Entity, "Concrete3", "str"));

        // Check for 2 attributes including key
        assertEquals(2, concrete4aEntity.size());
        // Check attributes
        assertEquals("blam4", getStrValue(concrete4aEntity, "Concrete4", "str"));

        // A reminder to write unit tests for dependent fields once the feature is supported
        HTable table = new HTable(Bytes.toBytes("Concrete4"));
        assertTrue(table.exists(new Get(Bytes.toBytes(concrete4b.getId()))));
        String concreteId = concrete.getId();
        pm = PMF.get().getPersistenceManager();
        try
        {
            concrete = pm.getObjectById(concrete.getClass(), concrete.getId());
            assertEquals("not base 1", concrete.getBase1Str());
            assertEquals("not concrete", concrete.getConcrete1Str());
            assertEquals(concrete3.getId(), concrete.getConcrete3().getId());
            assertEquals("blam3", concrete.getConcrete3().getStr());
            assertEquals(1, concrete.getConcrete4().size());
            assertEquals(concrete4a.getId(), concrete.getConcrete4().get(0).getId());
            assertEquals("blam4", concrete.getConcrete4().get(0).getStr());

            assertEquals(1,
                ((Collection<Concrete2>) pm.newQuery("select from " + concrete.getClass().getName() + " where base1Str == 'not base 1'")
                        .execute()).size());
            assertEquals(
                1,
                ((Collection<Concrete2>) pm.newQuery(
                    "select from " + concrete.getClass().getName() + " where concrete1Str == 'not concrete'").execute()).size());
            pm.deletePersistent(concrete);
        }
        finally
        {
            pm.close();
        }
        table = new HTable(Bytes.toBytes("Concrete1"));
        assertFalse(table.exists(new Get(Bytes.toBytes(concreteId))));

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testConcrete2() throws IOException, ClassNotFoundException
    {

        PersistenceManager pm = null;
        Concrete2 concrete = new Concrete2();
        concrete.setBase1Str("base 1");
        concrete.setBase2Str("base 2");
        concrete.setConcrete2Str("concrete");
        Concrete3 concrete3 = new Concrete3();
        concrete3.setStr("str3");
        concrete.setConcrete3(concrete3);
        Concrete4 concrete4a = new Concrete4();
        concrete4a.setStr("str4a");
        Concrete4 concrete4b = new Concrete4();
        concrete4b.setStr("str4b");
        concrete.setConcrete4(Utils.newArrayList(concrete4a, concrete4b));

        pm = PMF.get().getPersistenceManager();
        try
        {
            pm.makePersistent(concrete);
        }
        finally
        {
            pm.close();
        }

        Result concreteEntity = get("Concrete2", Bytes.toBytes(concrete.getId()));
        Result concrete3Entity = get("Concrete3", Bytes.toBytes(concrete3.getId()));
        Result concrete4aEntity = get("Concrete4", Bytes.toBytes(concrete4a.getId()));
        Result concrete4bEntity = get("Concrete4", Bytes.toBytes(concrete4b.getId()));
        assertEquals(6, concreteEntity.size());
        assertEquals("base 1", getStrValue(concreteEntity, "Concrete2", "base1Str"));
        assertEquals("base 2", getStrValue(concreteEntity, "Concrete2", "base2Str"));
        assertEquals("concrete", getStrValue(concreteEntity, "Concrete2", "concrete2Str"));
        StringIdentity concrete3Id = deserialize(StringIdentity.class, getBytes(concreteEntity, "Concrete2", "concrete3"));
        assertEquals(concrete3Id.getKey(), concrete3.getId());
        List<StringIdentity> concrete4Ids = deserialize(ArrayList.class, getBytes(concreteEntity, "Concrete2", "concrete4"));
        assertEquals(Utils.newArrayList(new StringIdentity(concrete4a.getClass(), concrete4a.getId()),
            new StringIdentity(concrete4b.getClass(), concrete4b.getId())), concrete4Ids);

        assertEquals(2, concrete3Entity.size());
        assertEquals("str3", getStrValue(concrete3Entity, "Concrete3", "str"));

        assertEquals(2, concrete4aEntity.size());
        assertEquals("str4a", getStrValue(concrete4aEntity, "Concrete4", "str"));

        assertEquals(2, concrete4bEntity.size());
        assertEquals("str4b", getStrValue(concrete4bEntity, "Concrete4", "str"));

        pm = PMF.get().getPersistenceManager();
        try
        {
            concrete = pm.getObjectById(concrete.getClass(), concrete.getId());
            assertEquals("base 1", concrete.getBase1Str());
            assertEquals("base 2", concrete.getBase2Str());
            assertEquals("concrete", concrete.getConcrete2Str());

            assertEquals(concrete3.getId(), concrete.getConcrete3().getId());
            assertEquals(concrete3.getStr(), concrete.getConcrete3().getStr());
            assertEquals(2, concrete.getConcrete4().size());
            assertEquals(concrete4a.getId(), concrete.getConcrete4().get(0).getId());
            assertEquals(concrete4a.getStr(), concrete.getConcrete4().get(0).getStr());
            assertEquals(concrete4b.getId(), concrete.getConcrete4().get(1).getId());
            assertEquals(concrete4b.getStr(), concrete.getConcrete4().get(1).getStr());

            concrete.setBase1Str("not base 1");
            concrete.setBase2Str("not base 2");
            concrete.setConcrete2Str("not concrete");

            concrete.getConcrete3().setStr("blam3");
            concrete.getConcrete4().get(0).setStr("blam4");
            concrete.getConcrete4().remove(1);
        }
        finally
        {
            pm.close();
        }

        concreteEntity = get("Concrete2", Bytes.toBytes(concrete.getId()));
        concrete3Entity = get("Concrete3", Bytes.toBytes(concrete3.getId()));
        concrete4aEntity = get("Concrete4", Bytes.toBytes(concrete4a.getId()));
        concrete4bEntity = get("Concrete4", Bytes.toBytes(concrete4b.getId()));

        assertEquals(6, concreteEntity.size());
        assertEquals("not base 1", getStrValue(concreteEntity, "Concrete2", "base1Str"));
        assertEquals("not base 2", getStrValue(concreteEntity, "Concrete2", "base2Str"));
        assertEquals("not concrete", getStrValue(concreteEntity, "Concrete2", "concrete2Str"));
        concrete3Id = deserialize(StringIdentity.class, getBytes(concreteEntity, "Concrete2", "concrete3"));
        assertEquals(concrete3Id.getKey(), concrete3.getId());
        concrete4Ids = deserialize(ArrayList.class, getBytes(concreteEntity, "Concrete2", "concrete4"));
        assertEquals(Utils.newArrayList(new StringIdentity(concrete4a.getClass(), concrete4a.getId())), concrete4Ids);

        assertEquals(2, concrete3Entity.size());
        assertEquals("blam3", getStrValue(concrete3Entity, "Concrete3", "str"));

        assertEquals(2, concrete4aEntity.size());
        assertEquals("blam4", getStrValue(concrete4aEntity, "Concrete4", "str"));
        // A reminder to write unit tests for dependent fields once the feature is supported
        HTable table = new HTable(Bytes.toBytes("Concrete4"));
        assertTrue(table.exists(new Get(Bytes.toBytes(concrete4b.getId()))));
        String concreteId = concrete.getId();
        pm = PMF.get().getPersistenceManager();
        try
        {
            concrete = pm.getObjectById(concrete.getClass(), concrete.getId());
            assertEquals("not base 1", concrete.getBase1Str());
            assertEquals("not base 2", concrete.getBase2Str());
            assertEquals("not concrete", concrete.getConcrete2Str());
            assertEquals(concrete3.getId(), concrete.getConcrete3().getId());
            assertEquals("blam3", concrete.getConcrete3().getStr());
            assertEquals(1, concrete.getConcrete4().size());
            assertEquals(concrete4a.getId(), concrete.getConcrete4().get(0).getId());
            assertEquals("blam4", concrete.getConcrete4().get(0).getStr());

            assertEquals(1,
                ((Collection<Concrete2>) pm.newQuery("select from " + concrete.getClass().getName() + " where base1Str == 'not base 1'")
                        .execute()).size());
            assertEquals(1,
                ((Collection<Concrete2>) pm.newQuery("select from " + concrete.getClass().getName() + " where base2Str == 'not base 2'")
                        .execute()).size());
            assertEquals(
                1,
                ((Collection<Concrete2>) pm.newQuery(
                    "select from " + concrete.getClass().getName() + " where concrete2Str == 'not concrete'").execute()).size());

            pm.deletePersistent(concrete);
        }
        finally
        {
            pm.close();
        }
        table = new HTable(Bytes.toBytes("Concrete2"));
        assertFalse(table.exists(new Get(Bytes.toBytes(concreteId))));
    }

}
